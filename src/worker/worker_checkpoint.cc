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

#include <cstdio>
#include <dirent.h>

#include <string>
#include <thread>
#include <chrono>
#include <algorithm>

using namespace std;

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(flush_time);

namespace dsm{

void Worker::initialCP(){
	checkpointing_=false;

	switch(kreq.cp_type()){
	case CP_NONE:
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_START);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_START);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_FINISH);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_FINISH);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC:
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
		RegDSPImmediate(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC_SIG:
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
//		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint, true);
		RegDSPImmediate(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPImmediate(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		break;
	case CP_ASYNC:
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
//		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint, true);
		RegDSPProcess(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		_cp_async_sig_rec.resize(config_.num_workers(),false);
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
		if(th_cp_)	th_cp_->join();
		delete th_cp_; th_cp_=nullptr;
		th_cp_=new thread(&Worker::_startCP_SyncSig,this);break;
	case CP_ASYNC:
		if(th_cp_)	th_cp_->join();
		delete th_cp_; th_cp_=nullptr;
		th_cp_=new thread(&Worker::_startCP_Async,this);break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}
//	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
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
		//flush message to other shards
		t->SendUpdates();
		//archive local state
		t->start_checkpoint(pre + "table-" + to_string(it->first));
	}
}
void Worker::_startCP_report(){
	CheckpointLocalDone rep;
	rep.set_wid(id());
	rep.set_epoch(epoch_);
	network_->Send(config_.master_id(),MTYPE_CHECKPOINT_LOCAL_DONE,rep);
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
	pause_pop_msg_=true;

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
	_startCP_report();
	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
}
void Worker::_finishCP_Sync(){
	_finishCP_common();
	pause_pop_msg_=false;
}

/*
 * Sync-Sig:
 */
void Worker::_startCP_SyncSig(){
	pause_pop_msg_=true;
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
	_startCP_report();
	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
}
void Worker::_finishCP_SyncSig(){
	_finishCP_common();
	pause_pop_msg_=false;
}
void Worker::_processCPSig_SyncSig(const int wid){
	const deque<pair<string, RPCInfo> >& que = driver.getQue();
	DVLOG(1)<<"flush signal process at "<<id()<<" for "<<wid;
	DVLOG(1)<<"queue length: "<<que.size();
	int count=0;
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(std::size_t i = 0; i < que.size(); ++i){
		//TODO: mapping from worker-id to network-id
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
	pause_pop_msg_=true;
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

	_startCP_common();
	RegDSPProcess(MTYPE_PUT_REQUEST,&Worker::_HandlePutRequest_AsynCP);
	fill(_cp_async_sig_rec.begin(),_cp_async_sig_rec.end(),false);
	pause_pop_msg_=false;
	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();

	rph.input(MTYPE_CHECKPOINT_SIG,id());	//input itself, others for incoming msg
	DVLOG(1)<<"wait for receiving all cp flush signals at "<<id();
	su_cp_sig.wait();
	su_cp_sig.reset();

	pause_pop_msg_=true;
	tmr_cp_block_.Reset();
	DVLOG(1)<<"received all cp flush signals at "<<id();
	RegDSPProcess(MTYPE_PUT_REQUEST,&Worker::HandlePutRequest);
	pause_pop_msg_=false;
	stats_["cp_time_blocked"]+=tmr_cp_block_.elapsed();
	_startCP_report();
}
void Worker::_finishCP_Async(){
	_finishCP_common();
}
void Worker::_processCPSig_Async(const int wid){
	_cp_async_sig_rec[wid]=true;
	rph.input(MTYPE_CHECKPOINT_SIG,wid);
}

void Worker::_HandlePutRequest_AsynCP(const string& d, const RPCInfo& info){
	KVPairData put;
	put.ParseFromString(d);

	ProcessPutRequest(put);

	//Difference:
	MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(
			TableRegistry::Get()->mutable_table(put.table()));
	if(!_cp_async_sig_rec[put.source()])
		t->write_message(put);
	DVLOG(1)<<"cp write a message from "<<put.source()<<" at worker "<<id();
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


void Worker::removeCheckpoint(const int epoch){
	string pre = FLAGS_checkpoint_write_dir + StringPrintf("/epoch_%04d/", epoch);
	DIR* dp = opendir(pre.c_str());
	if(dp==nullptr)
		return;

	struct dirent* ep;
	while ((ep = readdir(dp)) != nullptr) {
		VLOG(1)<<ep->d_name;
		if(ep->d_name[0]!='.'){
			string p=pre + ep->d_name;
			remove(p.c_str());
		}
	}

	closedir(dp);
	if(remove(pre.c_str())!=0)
		LOG(ERROR)<<"cannot delete checkpoint folder "<<pre;
}


} //namespace dsm
