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
//#include <functional>
#include <algorithm>

using namespace std;

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(flush_time);

namespace dsm{

void Worker::initialCP(){
	switch(kreq.cp_type()){
	case CP_NONE:
		driver.unregisterImmediateHandler(MTYPE_START_CHECKPOINT);
		driver.unregisterProcessHandler(MTYPE_START_CHECKPOINT);
		driver.unregisterImmediateHandler(MTYPE_FINISH_CHECKPOINT);
		driver.unregisterProcessHandler(MTYPE_FINISH_CHECKPOINT);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC:
		RegDSPProcess(MTYPE_START_CHECKPOINT, &Worker::HandleStartCheckpoint);
		RegDSPImmediate(MTYPE_FINISH_CHECKPOINT, &Worker::HandleFinishCheckpoint);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC_SIG:
		RegDSPProcess(MTYPE_START_CHECKPOINT, &Worker::HandleStartCheckpoint, true);
		RegDSPImmediate(MTYPE_FINISH_CHECKPOINT, &Worker::HandleFinishCheckpoint);
		RegDSPImmediate(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		break;
	case CP_ASYNC:
		RegDSPProcess(MTYPE_START_CHECKPOINT, &Worker::HandleStartCheckpoint, true);
		RegDSPProcess(MTYPE_FINISH_CHECKPOINT, &Worker::HandleFinishCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		_cp_async_msg_rec.resize(config_.num_workers(),false);
		break;
	default:
			LOG(ERROR)<<"given checkpoint type is not implemented.";
	}
}

bool Worker::startCheckpoint(const int epoch){
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
		tmr_.Reset();	//for checkpoint time statistics
		tmr_cp_block_.Reset();
		checkpointing_=true;
	}

	switch(kreq.cp_type()){
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

	switch(kreq.cp_type()){
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

		checkpointing_=false;
		stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
		stats_["cp_time"]+=tmr_.elapsed();
	}

	LOG(INFO) << "Finish worker checkpoint "<<epoch_<<" at W" << id();
	return true;
}

bool Worker::processCPSig(const int wid, const int epoch){
	if(epoch!=epoch_){
		LOG(INFO)<<"Skipping unmatched checkpoint flush signal: "<<epoch<<", curr="<<epoch_;
		return false;
	}
	tmr_cp_block_.Reset();
	switch(kreq.cp_type()){
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
	return true;
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
	//TODO: change peers_ to hold worker_id's net_id inside
	for(int i=1;i<network_->size();++i){
		if(i==network_->id())
			continue;
		DVLOG(1)<<"send checkpoint flush signal from "<<network_->id()<<" to "<<i;
		network_->Send(i,MTYPE_CHECKPOINT_SIG,req);
	}
	rph.input(MTYPE_CHECKPOINT_SIG,id());	//input itself, other for incoming msg
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
	driver_paused_=true;
	_startCP_common();
	RegDSPProcess(MTYPE_PUT_REQUEST,&Worker::_HandlePutRequest_AsynCP);
	CheckpointSyncSig req;
	req.set_wid(id());
	req.set_epoch(epoch_);
	//TODO: change peers_ to hold worker_id's net_id inside
	for(int i=1;i<network_->size();++i){
		if(i==network_->id())
			continue;
		DVLOG(1)<<"send checkpoint flush signal from "<<network_->id()<<" to "<<i;
		network_->Send(i,MTYPE_CHECKPOINT_SIG,req);
	}
	fill(_cp_async_msg_rec.begin(),_cp_async_msg_rec.end(),false);
	driver_paused_=false;

	rph.input(MTYPE_CHECKPOINT_SIG,id());	//input itself, other for incoming msg
	DVLOG(1)<<"wait for receiving all cp flush signals at "<<id();
	su_cp_sig.wait();
	DVLOG(1)<<"received all cp flush signals at "<<id();
	su_cp_sig.reset();
}
void Worker::_finishCP_Async(){
	RegDSPProcess(MTYPE_PUT_REQUEST,&Worker::HandlePutRequest);
	_finishCP_common();
}
void Worker::_processCPSig_Async(const int wid){
	_cp_async_msg_rec[wid]=true;
	rph.input(MTYPE_CHECKPOINT_SIG,wid);
}

void Worker::_HandlePutRequest_AsynCP(const string& d, const RPCInfo& info){
	KVPairData put;
	put.ParseFromString(d);

	DVLOG(2) << "Read put request of size: " << put.kv_data_size() << " for ("
				<< put.table()<<","<<put.shard()<<")";

	MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(
			TableRegistry::Get()->mutable_table(put.table()));
	t->MergeUpdates(put);
	t->ProcessUpdates();

	if(put.done() && t->tainted(put.shard())){
		VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
		t->get_partition_info(put.shard())->tainted = false;
	}

	//Difference:
	if(!_cp_async_msg_rec[put.source()])
		t->write_message(put);
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
