/*
 * worker_checkpoint.cc
 *
 *  Created on: Dec 26, 2015
 *      Author: tzhou
 */

#include "worker.h"
#include "net/Task.h"
#include "net/RPCInfo.h"
#include "net/NetworkThread.h"
#include "util/file.h"

#include <glog/logging.h>

#include <string>
#include <thread>
#include <chrono>
#include <functional>

using namespace std;

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(flush_time);

namespace dsm{


void Worker::checkpoint(const int epoch, const CheckpointType type){
	LOG(INFO) << "Begin worker checkpoint "<<epoch<<" at W" << id();
//	lock_guard<recursive_mutex> sl(state_lock_);
//	checkpoint_tables_.clear();
//	TableRegistry::Map &t = TableRegistry::Get()->tables();
//	for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
//		checkpoint_tables_.insert(make_pair(i->first, true));
//	}
	driver_paused_=true;
	DVLOG(1)<<driver.queSize();
	startCheckpoint(epoch, type);
	finishCheckpoint(epoch);
	DVLOG(1)<<driver.queSize();
	driver_paused_=false;
	LOG(INFO) << "Finish worker checkpoint "<<epoch<<" at W" << id();

//	UpdateEpoch(int peer, int peer_epoch);
}

bool Worker::startCheckpoint(const int epoch, const CheckpointType type){
	LOG(INFO) << "Begin worker checkpoint "<<epoch<<" at W" << id();
	if(epoch_ >= epoch){
		LOG(INFO)<< "Skip old checkpoint request: "<<epoch<<", curr="<<epoch_;
		return false;
	}else if(checkpointing_){
		LOG(INFO)<<"Skip current checkpoint request because last one has not finished. "<<epoch<<" vs "<<epoch_;
		return false;
	}

	{
		lock_guard<recursive_mutex> sl(state_lock_);

		epoch_ = epoch;
		active_checkpoint_ = type;
		tmr_.Reset();	//for checkpoint time statistics
		tmr_cp_block_.Reset();
		checkpointing_=true;
	}

	switch(active_checkpoint_){
	case CP_SYNC:
		_startCP_Sync();break;
	case CP_SYNC_SIG:
		_startCP_SyncSig();break;
	case CP_ASYNC:
		_startCP_Async();break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}

	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
	return true;
}

bool Worker::finishCheckpoint(const int epoch){
	if(epoch!=epoch_){
		LOG(INFO)<< "Skip unmatched checkpoint finish request: "<<epoch<<", curr="<<epoch_;
		return false;
	}
	tmr_cp_block_.Reset();

	switch(active_checkpoint_){
	case CP_SYNC:
		_finishCP_Sync();break;
	case CP_SYNC_SIG:
		_finishCP_SyncSig();break;
	case CP_ASYNC:
		_finishCP_Async();break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}

	{
		lock_guard<recursive_mutex> sl(state_lock_);

		for(int i = 0; i < peers_.size(); ++i){
			peers_[i]->epoch = epoch_;
		}

		active_checkpoint_ = CP_NONE;
		stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
		stats_["cp_time"]+=tmr_.elapsed();
	}

	checkpointing_=false;
	LOG(INFO) << "Finish worker checkpoint "<<epoch_<<" at W" << id();
	return true;
}

void Worker::processCPSig(const int wid, const int epoch){
	if(epoch!=epoch_){
		LOG(INFO)<<"Skipping unmatched checkpoint flush signal: "<<epoch<<", curr="<<epoch_;
		return;
	}
	tmr_cp_block_.Reset();
	switch(active_checkpoint_){
	case CP_SYNC:
		break;
	case CP_SYNC_SIG:
		_processCPSig_SyncSig(wid);break;
	case CP_ASYNC:
		_processCPSig_Async(wid);break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}
	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
}

void Worker::_startCP_common(){
	//create folder for storing this checkpoint
	string pre = FLAGS_checkpoint_write_dir + StringPrintf("/epoch_%04d/", epoch_);
	File::Mkdirs(pre);

	//archive current table state:
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = tbl.begin(); it != tbl.end(); ++it){
		VLOG(1) << "Starting checkpoint... on table " << it->first;
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(it->second);
		//flush message for other shards
		t->SendUpdates();
		//archive local state
		t->start_checkpoint(pre + "table-" + to_string(it->first));
	}
}
void Worker::_finishCP_common(){
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tbl.begin(); i != tbl.end(); ++i){
		Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->finish_checkpoint();
		}
	}
}

/*
 * Sync:
 */

void Worker::_startCP_Sync(){
	driver_paused_=true;

	//archive current table state:
	_startCP_common();

	//archive message in the waiting queue
	this_thread::sleep_for(chrono::duration<double>(FLAGS_flush_time));

	const deque<pair<string, RPCInfo> >& que = driver.getQue();
	DVLOG(1)<<"queue length: "<<que.size();
	int count=0;
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(std::size_t i = 0; i < que.size(); ++i){
		if(que[i].second.tag == MTYPE_PUT_REQUEST){
			KVPairData d;
			d.ParseFromString(que[i].first);
			Checkpointable *t = dynamic_cast<Checkpointable*>(tbl.at(d.table()));
//			DVLOG(1)<<"message: "<<d.kv_data(0).DebugString();
			++count;
			t->write_message(d);
		}
	}
	DVLOG(1)<<"archived msg: "<<count;
}
void Worker::_finishCP_Sync(){
	_finishCP_common();
	driver_paused_=false;
}

/*
 * Sync-Sig:
 */
void Worker::_startCP_SyncSig(){
	driver_paused_=true;
	_startCP_common();
	CheckpointSyncSig req;
	req.set_wid(id());
	req.set_epoch(epoch_);
//	req.set_type(active_checkpoint_);
	//TODO: change peers_ to hold worker_id's net_id inside
	for(int i=1;i<network_->size();++i){
		if(i==network_->id())
			continue;
		DVLOG(1)<<"send checkpoint flush signal from "<<network_->id()<<" to "<<i;
		network_->Send(i,MTYPE_CHECKPOINT_SIG,req);
	}
	rph.input(MTYPE_CHECKPOINT_SIG,id());	//input itself, other for incoming msg
//	if(id()==0)
//		rph.input(MTYPE_CHECKPOINT_SIG,1);
	DVLOG(1)<<"wait for receiving all cp flush signals at "<<id();
	su_cp_sig.wait();
	DVLOG(1)<<"received all cp flush signals at "<<id();
	su_cp_sig.reset();
}
void Worker::_finishCP_SyncSig(){
	_finishCP_common();
	driver_paused_=false;
}
void Worker::_processCPSig_SyncSig(const int wid){
	const deque<pair<string, RPCInfo> >& que = driver.getQue();
	DVLOG(1)<<"flush signal process at "<<id()<<" for "<<wid;
	DVLOG(1)<<"queue length: "<<que.size();
	int count=0;
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(std::size_t i = 0; i < que.size(); ++i){
		if(que[i].second.tag == MTYPE_PUT_REQUEST && que[i].second.source==wid+1){
			KVPairData d;
			d.ParseFromString(que[i].first);
			Checkpointable *t = dynamic_cast<Checkpointable*>(tbl.at(d.table()));
//			DVLOG(1)<<"message: "<<d.kv_data(0).DebugString();
			++count;
			t->write_message(d);
		}
	}
	DVLOG(1)<<"archived msg: "<<count;
	rph.input(MTYPE_CHECKPOINT_SIG,wid);
}

/*
 * Async:
 */
void Worker::_startCP_Async(){

}
void Worker::_finishCP_Async(){

}
void Worker::_processCPSig_Async(const int wid){

}

/*
 * restore:
 */
void Worker::restore(int epoch){
	lock_guard<recursive_mutex> sl(state_lock_);
	LOG(INFO)<< "Worker restoring state from epoch: " << epoch;
	epoch_ = epoch;

	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable* t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->restore(FLAGS_checkpoint_read_dir+
					StringPrintf("/epoch_%04d/checkpoint.table-%d",epoch_, i->first));
		}
	}
}


} //namespace dsm
