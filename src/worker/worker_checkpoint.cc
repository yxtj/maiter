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

void Worker::startCheckpoint(const int epoch, const CheckpointType type){
	LOG(INFO) << "Begin worker checkpoint "<<epoch<<" at W" << id();
	if(epoch_ >= epoch){
		LOG(INFO)<< "Skipping old checkpoint request: " <<epoch;
		return;
	}

	{
		lock_guard<recursive_mutex> sl(state_lock_);

		active_checkpoint_ = type;
		epoch_ = epoch;
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

}

void Worker::finishCheckpoint(){
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
	}

	LOG(INFO) << "Finish worker checkpoint "<<epoch_<<" at W" << id();
}

void Worker::_startCP_Sync(){
	driver_paused_=true;

	string pre = FLAGS_checkpoint_write_dir + StringPrintf("/epoch_%04d/", epoch_);
	File::Mkdirs(pre);

	//archive current table state:
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = t.begin(); it != t.end(); ++it){
		VLOG(1) << "Starting checkpoint... on table " << it->first;
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(it->second);
		//flush message for other shards
		t->SendUpdates();
		//archive local state
		t->start_checkpoint(pre + "table-" + to_string(it->first));
	}

	//archive message in the waiting queue
	this_thread::sleep_for(chrono::duration<double>(FLAGS_flush_time));

	const deque<pair<string, RPCInfo> >& que = driver.getQue();
	DVLOG(1)<<"queue length: "<<que.size();
	int count=0;
	for(TableRegistry::Map::iterator it = t.begin(); it != t.end(); ++it){
		Checkpointable *t = dynamic_cast<Checkpointable*>(it->second);
		for(std::size_t i = 0; i < que.size(); ++i){
			if(que[i].second.tag == MTYPE_PUT_REQUEST){
				KVPairData d;
				d.ParseFromString(que[i].first);
				if(d.table()==it->first){
//					DVLOG(1)<<"message: "<<d.kv_data(0).DebugString();
					++count;
					t->write_message(d);
				}
			}
		}
	}
	DVLOG(1)<<"archived msg: "<<count;
}
void Worker::_finishCP_Sync(){
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->finish_checkpoint();
		}
	}
	driver_paused_=false;
}

void Worker::_startCP_SyncSig(){

}
void Worker::_finishCP_SyncSig(){

}

void Worker::_startCP_Async(){

}
void Worker::_finishCP_Async(){

}

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
	finishCheckpoint();
	DVLOG(1)<<driver.queSize();
	driver_paused_=false;
	LOG(INFO) << "Finish worker checkpoint "<<epoch<<" at W" << id();

//	UpdateEpoch(int peer, int peer_epoch);
}

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
