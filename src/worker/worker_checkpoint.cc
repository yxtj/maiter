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
#include "table/table-registry.h"

#include <glog/logging.h>

#include <cstdio>
//#include <dirent.h>

#include <string>
#include <fstream>
#include <thread>
#include <chrono>
#include <algorithm>

using namespace std;

DECLARE_string(checkpoint_dir);
DECLARE_double(flush_time);

DECLARE_int32(taskid);

namespace dsm{

void Worker::initialCP(CheckpointType cpType){
	st_checkpointing_=false;
	pause_pop_msg_=true;

	switch(cpType){
	case CP_NONE:
		VLOG(1)<<"register none cp handler";
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_START);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_START);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_FINISH);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_FINISH);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC:
		VLOG(1)<<"register sync cp handler";
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
		//RegDSPImmediate(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		driver.unregisterImmediateHandler(MTYPE_CHECKPOINT_SIG);
		driver.unregisterProcessHandler(MTYPE_CHECKPOINT_SIG);
		break;
	case CP_SYNC_SIG:
		VLOG(1)<<"register sync_sig cp handler";
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
		RegDSPImmediate(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
//		RegDSPProcess(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPImmediate(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		break;
	case CP_ASYNC:
		VLOG(1)<<"register async cp handler";
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
//		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint, true);
		RegDSPProcess(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		break;
	case CP_VS:
		VLOG(1) << "register vistural snapshot cp handler";
		RegDSPProcess(MTYPE_CHECKPOINT_START, &Worker::HandleStartCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_FINISH, &Worker::HandleFinishCheckpoint);
		RegDSPProcess(MTYPE_CHECKPOINT_SIG, &Worker::HandleCheckpointSig);
		break;
	default:
		LOG(FATAL)<<"given checkpoint type is not implemented.";
	}

	_cp_async_sig_rec.assign(config_.num_workers(), false);
	pause_pop_msg_=false;
}

bool Worker::startCheckpoint(const int epoch){
	LOG(INFO) << "Begin worker checkpoint "<<epoch<<" at W" << id();
	if(epoch_ >= epoch){
		LOG(INFO)<< "Skip old checkpoint request: "<<epoch<<", curr="<<epoch_;
		return false;
	}else if(st_checkpointing_){
		LOG(INFO)<<"Skip current checkpoint request because last one has not finished. "<<epoch<<" vs "<<epoch_;
		return false;
	}

	{
		lock_guard<recursive_mutex> sl(state_lock_);

		epoch_ = epoch;
		tmr_.Reset();	//for checkpoint time statistics
		tmr_cp_block_.Reset();
		st_checkpointing_=true;
	}

	switch(kreq.cp_type()){
	case CP_SYNC:
		_startCP_Sync();break;
	case CP_SYNC_SIG:
//		if(th_cp_)	th_cp_->join();
//		delete th_cp_; th_cp_=nullptr;
//		th_cp_=new thread(&Worker::_startCP_SyncSig,this);break;
		_startCP_SyncSig();break;
	case CP_ASYNC:
//		if(th_cp_)	th_cp_->join();
//		delete th_cp_; th_cp_=nullptr;
//		th_cp_=new thread(&Worker::_startCP_Async,this);break;
		_startCP_Async();break;
	case CP_VS:
		_startCP_VS();
		break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}
	stats_["time_cp"]+=tmr_.elapsed();
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
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
		_finishCP_Sync();
		break;
	case CP_SYNC_SIG:
		if(th_cp_)	th_cp_->join();
		delete th_cp_; th_cp_=nullptr;
		th_cp_=new thread(&Worker::_finishCP_SyncSig,this);
		break;
		//_finishCP_SyncSig();break;
	case CP_ASYNC:
		//if(th_cp_)	th_cp_->join();
		//delete th_cp_; th_cp_=nullptr;
		//th_cp_=new thread(&Worker::_finishCP_Async,this);
		//break;
		_finishCP_Async();break;
	case CP_VS:
		_finishCP_VS();
		break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}

	{
		lock_guard<recursive_mutex> sl(state_lock_);

		for(int i = 0; i < peers_.size(); ++i){
			peers_[i].epoch = epoch_;
		}

		st_checkpointing_=false;
//		if(kreq.cp_type()==CP_SYNC){
//			stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
//		}
		stats_["time_cp"]+=tmr_.elapsed();
	}

	LOG(INFO) << "Finish worker checkpoint "<<epoch_<<" at W" << id();
	return true;
}

bool Worker::processCPSig(const int wid, const int epoch){
	tmr_cp_block_.Reset();
	switch(kreq.cp_type()){
	case CP_SYNC:
		break;
	case CP_SYNC_SIG:
		_processCPSig_SyncSig(wid);break;
	case CP_ASYNC:
		_processCPSig_Async(wid);break;
	case CP_VS:
		_processCPSig_VS(wid); break;
	default:
		LOG(ERROR)<<"given checkpoint type is not implemented.";
	}
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
	return true;
}

void Worker::_CP_start(){
	//create folder for storing this checkpoint
	string pre = _CP_local_file_name(epoch_);
	//File::Mkdirs(pre);

	//archive current table state:
	TableRegistry::Map &tbls = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = tbls.begin(); it != tbls.end(); ++it){
		VLOG(1) << "Starting checkpoint on W" << id();
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(it->second);
		//flush message to other shards
		t->SendUpdates();
		//archive local state
		Timer tmr;
		t->start_checkpoint(pre);
		stats_["time_cp_archive"] += tmr.elapsed();
	}
}
void Worker::_sendCPFlushSig(bool self){
	CheckpointSyncSig req;
	req.set_wid(id());
	req.set_epoch(epoch_);
	for(size_t i=0;i<peers_.size();++i){
		if(!self && i==id())
			continue;
		DVLOG(1)<<"send checkpoint flush signal from "<<id()<<" to "<<i;
		network_->Send(peers_[i].net_id,MTYPE_CHECKPOINT_SIG,req);
	}
}
void Worker::_CP_report(){
	CheckpointLocalDone rep;
	rep.set_wid(id());
	rep.set_epoch(epoch_);
	DVLOG(1) << "report checkpoint local finsih from " << id();
	network_->Send(config_.master_id(),MTYPE_CHECKPOINT_LOCAL_DONE,rep);
}
void Worker::_CP_stop(){
	Timer tmr;
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tbl.begin(); i != tbl.end(); ++i){
		VLOG(1) << "Finishing checkpoint on W" << id();
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(i->second);
		if(t){
			t->finish_checkpoint();
		}
	}
	stats_["time_cp_archive"] += tmr.elapsed();
	_enableProcess();
}

void Worker::_CP_dump_gtables()
{
	VLOG(1) << "W" << config_.worker_id() << " dump checkpoint of epoch: " << epoch_;
	Timer tmr;
	string fn = _CP_local_file_name(epoch_);
	//CHECK(fout.good()) << "failed in opening checkpoint file: " << fn;
	//archive current table state:
	TableRegistry::Map& tbls = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = tbls.begin(); it != tbls.end(); ++it){
		MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*>(it->second);
		//archive local state
		t->dump(fn, nullptr);
	}
	stats_["time_cp_archive"] += tmr.elapsed();
}

std::string Worker::_CP_local_file_name(int epoch)
{	
	string fn;
	TableRegistry::Map &tbls = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tbls.begin(); i != tbls.end(); ++i){
		MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*>(i->second);
		if(t){
			for(int s = 0; s < t->num_shards(); ++s){
				if(t->is_local_shard(s)){
					fn = FLAGS_checkpoint_dir + "/" + genCPName(FLAGS_taskid, epoch, s);
					break;
				}
			}
		}
	}
	return fn;
}

/*
 * Sync:
 */

void Worker::_startCP_Sync(){
//	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
	_disableProcess();
	_disableSend();
}
void Worker::_finishCP_Sync(){
	//pause_pop_msg_=true;

	//archive current table state:
	//_CP_start();

	//archive message in the waiting queue
	while(network_->active())
		this_thread::sleep_for(chrono::duration<double>(FLAGS_flush_time));
	this_thread::sleep_for(chrono::duration<double>(FLAGS_flush_time));

	//const deque<pair<string, RPCInfo> >& que = driver.getQue();
	//DVLOG(1)<<"queue length: "<<que.size();

	_CP_dump_gtables();
	/*
	int count=0;
	TableRegistry::Map& tbls = TableRegistry::Get()->tables();
	for(std::size_t i = 0; i < que.size(); ++i){
		if(que[i].second.tag == MTYPE_PUT_REQUEST){
			KVPairData d;
			d.ParseFromString(que[i].first);
			Checkpointable *t = dynamic_cast<Checkpointable*>(tbls.at(d.table()));
			//DVLOG(1)<<"message: "<<d.kv_data(0).DebugString();
			++count;
			t->write_message(d);
		}
	}
	DVLOG(1)<<"archived msg: "<<count;
	*/
	//_CP_stop();

	//resume
	_enableSend();
	_enableProcess();
	//pause_pop_msg_=false;
	_CP_report();
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
}

/*
 * Sync-Sig:
 */
void Worker::_startCP_SyncSig(){
	pause_pop_msg_=true;
	_disableProcess();
//	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
}
void Worker::_finishCP_SyncSig(){
	_CP_start();
	_sendCPFlushSig();	//send this sig after every workers have updated their epoch_
	rph.input(MTYPE_CHECKPOINT_SIG,id());	//input itself, other for incoming msg
	DVLOG(1)<<"wait for receiving all cp flush signals at "<<id();
	su_cp_sig.wait();
	DVLOG(1)<<"received all cp flush signals at "<<id();
	su_cp_sig.reset();
	_CP_stop();
	_enableProcess();
	pause_pop_msg_=false;
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
	_CP_report();
}
void Worker::_processCPSig_SyncSig(const int wid){
	const deque<pair<string, RPCInfo> >& que = driver.getQue();
	DVLOG(1)<<"flush signal processd at "<<id()<<" for "<<wid;
	DVLOG(1)<<"queue length: "<<que.size();
	Timer tmr;
	int count=0;
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(std::size_t i = 0; i < que.size(); ++i){
		if(que[i].second.tag == MTYPE_PUT_REQUEST && que[i].second.source==peers_[wid].net_id){
			KVPairData d;
			d.ParseFromString(que[i].first);
			Checkpointable *t = dynamic_cast<Checkpointable*>(tbl.at(d.table()));
			//	ofstream fout("haha/"+to_string(id())+'-'+to_string(wid));
			//	fout<<d.DebugString();
			++count;
			t->write_message(d);
		}
	}
	stats_["count_cp_message"] += count;
	stats_["time_cp_archive"] += tmr.elapsed();
	DVLOG(1)<<"archived msg: "<<count;
	rph.input(MTYPE_CHECKPOINT_SIG,wid);
}

/*
 * Async:
 */
void Worker::_startCP_Async(){
	//synchronize the starting signal
//	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
}
void Worker::_finishCP_Async(){
	pause_pop_msg_=true;
	_disableProcess();
	// Because local checkpoint stores unprocessed delta (future out-going
	// message) together with current value, we need not to process and send
	// then before sending the flush signal.
	_sendCPFlushSig(true);
	_CP_start();
	{
		lock_guard<mutex> lg(m_cp_control_);
		RegDSPProcess(MTYPE_PUT_REQUEST, &Worker::_HandlePutRequest_AsynCP);
	}
	_enableProcess();
	pause_pop_msg_=false;
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();

	DVLOG(1)<<"wait for receiving all cp flush signals at W"<<id();
	//su_cp_sig.wait();
	//su_cp_sig.reset();
}
void Worker::_processCPSig_Async(const int wid){
	lock_guard<mutex> lg(m_cp_control_);
	_cp_async_sig_rec[wid]=true;
	if(VLOG_IS_ON(2)){
		string s;
		for(bool b : _cp_async_sig_rec)
			s += to_string(b) + " ";
		DVLOG(2) << "W" << id() << " receive sig from " << wid << ". state: " << s;
	}
	rph.input(MTYPE_CHECKPOINT_SIG,wid);

	if(all_of(_cp_async_sig_rec.begin(), _cp_async_sig_rec.end(), [](bool v){return v; })){
		tmr_cp_block_.Reset();
		//pause_pop_msg_=true;
		DVLOG(1)<<"received all cp flush signals at "<<id();
		_cp_async_sig_rec.assign(config_.num_workers(), false);
		{
			//lock_guard<mutex> lg(m_cp_control_);
			RegDSPProcess(MTYPE_PUT_REQUEST, &Worker::HandlePutRequest);
			_CP_report();
			_CP_stop();
		}
		//pause_pop_msg_=false;
		stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
	}
}

void Worker::_HandlePutRequest_AsynCP(const string& d, const RPCInfo& info){
	KVPairData put;
	put.ParseFromString(d);
	lock_guard<mutex> lg(m_cp_control_);

	HandlePutRequestReal(put);

	//Difference:
	tmr_cp_block_.Reset();
	MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(
			TableRegistry::Get()->mutable_table(put.table()));
	if(!_cp_async_sig_rec[put.source()]){
		t->write_message(put);
	}
	DVLOG(1) << "W" << id() << " write a cp message from " << put.source();
	stats_["count_cp_message"] += 1;
	stats_["time_cp_archive"] += tmr_cp_block_.elapsed();
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
}

/*
 * virtual snapshot:
 */

void Worker::_startCP_VS()
{
	_cp_async_sig_rec[config_.worker_id()] = true;
	_disableSend();
	_sendCPFlushSig(true);
}

void Worker::_finishCP_VS()
{
	_cp_async_sig_rec.assign(config_.num_workers(), false);
	//_cp_async_sig_rec[config_.worker_id()] = true;
	_enableSend();
	stats_["time_cp_blocked"]+=tmr_cp_block_.elapsed();
}

void Worker::_processCPSig_VS(const int wid)
{
	DVLOG(1) << "W" << config_.worker_id() << " receive SIG from " << wid;
	_cp_async_sig_rec[wid] = true;
	//rph.input(MTYPE_CHECKPOINT_SIG, wid);
	if(all_of(_cp_async_sig_rec.begin(), _cp_async_sig_rec.end(), [](bool v){return v;})){
		_disableProcess();
		_CP_dump_gtables();
		_CP_report();
		_enableProcess();
	}
}

/*
 * restore:
 */
void Worker::restore(int epoch){
	lock_guard<recursive_mutex> sl(state_lock_);
	LOG(INFO)<< "W"<<id()<<" is restoring state from epoch: " << epoch;

	epoch_ = epoch;
	string fn = _CP_local_file_name(epoch);

	TableRegistry::Map &tbls = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tbls.begin(); i != tbls.end(); ++i){
		MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*>(i->second);
		if(t){
			if(kreq.cp_type() == CP_SYNC || kreq.cp_type() == CP_VS){
				//ifstream fin(fn);
				//CHECK(fin) << "Cannot open checkpoint file: " << fn;
				t->restore(fn, nullptr);
			} else{
				t->load_checkpoint(fn);
			}
		}
	}
	LOG(INFO)<< "Worker "<<id()<<" has restored state from epoch: " << epoch;
}


void Worker::removeCheckpoint(const int epoch){
	/*
	string pre = FLAGS_checkpoint_dir +
		"/" + genCPNameFolderPart(FLAGS_taskid, epoch) + "/";
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
	*/
}


} //namespace dsm
