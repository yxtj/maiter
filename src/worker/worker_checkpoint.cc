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
#include <string>
#include <thread>
#include <chrono>
#include <functional>

#include <glog/logging.h>

using namespace std;

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);

namespace dsm{

void Worker::StartCheckpoint(const int epoch, const CheckpointType type){
	lock_guard<recursive_mutex> sl(state_lock_);

	if(epoch_ >= epoch){
		LOG(INFO)<< "Skipping checkpoint; " << MP(epoch_, epoch);
		return;
	}

	epoch_ = epoch;

	File::Mkdirs(StringPrintf("%s/epoch_%05d/", FLAGS_checkpoint_write_dir.c_str(), epoch_));

	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		if(checkpoint_tables_.find(i->first) != checkpoint_tables_.end()){
			VLOG(1) << "Starting checkpoint... " << MP(id(), epoch_, epoch) << " : " << i->first;
			Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
			CHECK(t != NULL) << "Tried to checkpoint a read-only table?";

			t->start_checkpoint(
					StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
							FLAGS_checkpoint_write_dir.c_str(), epoch_, i->first));
		}
	}

	active_checkpoint_ = type;

	// For rolling checkpoints, send out a marker to other workers indicating
	// that we have switched epochs.
	if(type == CP_ROLLING){
		TableData epoch_marker;
		epoch_marker.set_source(id());
		epoch_marker.set_table(-1);
		epoch_marker.set_shard(-1);
		epoch_marker.set_done(true);
		epoch_marker.set_marker(epoch_);
		for(int i = 0; i < peers_.size(); ++i){
			network_->Send(i + 1, MTYPE_PUT_REQUEST, epoch_marker);
		}
	}

	VLOG(1) << "Starting delta logging... " << MP(id(), epoch_, epoch);
}

void Worker::FinishCheckpoint(){
	VLOG(1) << "Worker " << id() << " flushing checkpoint.";
	lock_guard<recursive_mutex> sl(state_lock_);

	active_checkpoint_ = CP_NONE;
	TableRegistry::Map &t = TableRegistry::Get()->tables();

	for(int i = 0; i < peers_.size(); ++i){
		peers_[i]->epoch = epoch_;
	}

	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->finish_checkpoint();
		}
	}
}

void Worker::checkpoint(const int epoch, const CheckpointType type){
	LOG(INFO)<< "Start Checkpointing at Worker "<<id()<<", with epoch "<<epoch;
	lock_guard<recursive_mutex> sl(state_lock_);
	checkpoint_tables_.clear();
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		checkpoint_tables_.insert(make_pair(i->first, true));
	}
	StartCheckpoint(epoch, type);
	FinishCheckpoint();

//	UpdateEpoch(int peer, int peer_epoch):
//	VLOG(1) << "Got peer marker: " << MP(peer, MP(epoch_, peer_epoch));
//	if(epoch_ < peer_epoch){
//		LOG(INFO)<< "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);
//		checkpoint_tables_.clear();
//		TableRegistry::Map &t = TableRegistry::Get()->tables();
//		for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
//			checkpoint_tables_.insert(make_pair(i->first, true));
//		}
//		StartCheckpoint(peer_epoch, CP_ROLLING);
//	}
//	peers_[peer]->epoch = peer_epoch;
//	bool checkpoint_done = true;
//	for(int i = 0; i < peers_.size(); ++i){
//		if(peers_[i]->epoch != epoch_){
//			checkpoint_done = false;
//			VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
//		}
//	}
//	if(checkpoint_done){
//		FinishCheckpoint();
//	}
}

void Worker::Restore(int epoch){
	lock_guard<recursive_mutex> sl(state_lock_);
	LOG(INFO)<< "Worker restoring state from epoch: " << epoch;
	epoch_ = epoch;

	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable* t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->restore(
					StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
							FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
		}
	}

	EmptyMessage req;
	network_->Send(config_.master_id(), MTYPE_RESTORE_DONE, req);
}


} //namespace dsm
