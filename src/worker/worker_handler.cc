/*
 * worker_handler.cc
 *
 *  Created on: Dec 16, 2015
 *      Author: tzhou
 */

#include "worker.h"
#include "net/Task.h"
#include "net/RPCInfo.h"
#include "net/NetworkThread2.h"
#include <string>
#include <thread>
#include <chrono>
#include <functional>

#include <glog/logging.h>

using namespace std;
using namespace std::placeholders;

namespace dsm{

void Worker::RegDSPImmediate(const int type, callback_t fp, bool spawnThread){
	driver.registerImmediateHandler(type, bind(fp, this, _1, _2), spawnThread);
}
void Worker::RegDSPProcess(const int type, callback_t fp, bool spawnThread){
	driver.registerProcessHandler(type, bind(fp, this, _1, _2), spawnThread);
}
void Worker::RegDSPDefault(callback_t fp){
	driver.registerDefaultOutHandler(bind(fp, this, _1, _2));
}

void Worker::registerHandlers(){
	RegDSPImmediate(MTYPE_SHARD_ASSIGNMENT, &Worker::HandleShardAssignment2);
	RegDSPImmediate(MTYPE_CLEAR_TABLE, &Worker::HandleClearRequest2);
	RegDSPImmediate(MTYPE_SWAP_TABLE, &Worker::HandleSwapRequest2);
	RegDSPImmediate(MTYPE_WORKER_FLUSH, &Worker::HandleFlush2, true);
	RegDSPImmediate(MTYPE_WORKER_APPLY, &Worker::HandleApply2);
	RegDSPImmediate(MTYPE_ENABLE_TRIGGER, &Worker::HandleEnableTrigger2);
	RegDSPImmediate(MTYPE_TERMINATION, &Worker::HandleTermNotification2);

	RegDSPImmediate(MTYPE_RUN_KERNEL,&Worker::HandleRunKernel2,true);
	RegDSPImmediate(MTYPE_WORKER_SHUTDOWN, &Worker::HandleShutdown2);
	RegDSPImmediate(MTYPE_REPLY, &Worker::HandleReply);

	RegDSPProcess(MTYPE_PUT_REQUEST, &Worker::HandlePutRequest2);
	return;

//	RegisterCallback2(MTYPE_SHARD_ASSIGNMENT, &Worker::HandleShardAssignment2, this);
//	RegisterCallback2(MTYPE_CLEAR_TABLE, &Worker::HandleClearRequest2, this);
//	RegisterCallback2(MTYPE_SWAP_TABLE, &Worker::HandleSwapRequest2, this);
//	RegisterCallback2(MTYPE_WORKER_FLUSH, &Worker::HandleFlush2, this);
//	network_->SpawnThreadFor(MTYPE_WORKER_FLUSH);
//	RegisterCallback2(MTYPE_WORKER_APPLY, &Worker::HandleApply2, this);
//	RegisterCallback2(MTYPE_ENABLE_TRIGGER, &Worker::HandleEnableTrigger2, this);
//	RegisterCallback2(MTYPE_TERMINATION, &Worker::HandleTermNotification2, this);
//
//	RegisterCallback2(MTYPE_RUN_KERNEL,&Worker::HandleRunKernel2,this);
//	RegisterCallback2(MTYPE_WORKER_SHUTDOWN, &Worker::HandleShutdown2, this);
//	RegisterCallback2(MTYPE_REPLY, &Worker::HandleReply, this);
//	RegisterCallback2(MTYPE_PUT_REQUEST, &Worker::HandlePutRequest2, this);
}

void Worker::HandlePutRequest2(const string& d, const RPCInfo& info){
	KVPairData put;
	put.ParseFromString(d);
	if(put.marker() != -1){
		UpdateEpoch(put.source(), put.marker());
		return;
	}

	DVLOG(1) << "Read put request of size: " << put.kv_data_size() << " for ("
				<< put.table()<<","<<put.shard()<<")";

	MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(put.table());
	t->MergeUpdates(put);

	if(put.done() && t->tainted(put.shard())){
		VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
		t->get_partition_info(put.shard())->tainted = false;
	}
}


void Worker::HandleSwapRequest2(const string& d, const RPCInfo& rpc){
	SwapTable req;
	req.ParseFromString(d);
	MutableGlobalTableBase *ta = TableRegistry::Get()->mutable_table(req.table_a());
	MutableGlobalTableBase *tb = TableRegistry::Get()->mutable_table(req.table_b());
	ta->local_swap(tb);

	sendReply(rpc);
}

void Worker::HandleClearRequest2(const string& d, const RPCInfo& rpc){
	ClearTable req;
	req.ParseFromString(d);
	MutableGlobalTableBase *ta = TableRegistry::Get()->mutable_table(req.table());
	for(int i = 0; i < ta->num_shards(); ++i){
		if(ta->is_local_shard(i)){
			ta->get_partition(i)->clear();
		}
	}
	sendReply(rpc);
}

void Worker::HandleShardAssignment2(const string& d,const RPCInfo& rpc){
	ShardAssignmentRequest shard_req;
	shard_req.ParseFromString(d);
//  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
	for(int i = 0; i < shard_req.assign_size(); ++i){
		const ShardAssignment &a = shard_req.assign(i);
		GlobalTableBase *t = TableRegistry::Get()->table(a.table());
		int old_owner = t->owner(a.shard());
		t->get_partition_info(a.shard())->sinfo.set_owner(a.new_worker());

		VLOG(3) << "Setting owner: " << MP(a.shard(), a.new_worker());

		if(a.new_worker() == id() && old_owner != id()){
			VLOG(1) << "Setting self as owner of " << MP(a.table(), a.shard());

			// Don't consider ourselves canonical for this shard until we receive updates
			// from the old owner.
			if(old_owner != -1){
				LOG(INFO)<< "Setting " << MP(a.table(), a.shard())
				<< " as tainted.  Old owner was: " << old_owner
				<< " new owner is :  " << id();
				t->get_partition_info(a.shard())->tainted = true;
			}
		} else if (old_owner == id() && a.new_worker() != id()){
			VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
			// A new worker has taken ownership of this shard.  Flush our data out.
			t->get_partition_info(a.shard())->dirty = true;
			dirty_tables_.insert(t);
		}
	}
	sendReply(rpc);
}

void Worker::HandleFlush2(const string& d, const RPCInfo& rpc){
	Timer net;
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(t){
			t->SendUpdates();
		}
	}

	network_->Flush();
	stats_["network_time"] += net.elapsed();
	sendReply(rpc);
}


void Worker::HandleApply2(const string& d, const RPCInfo& rpc){
	HandlePutRequest();
	sendReply(rpc);
}

void Worker::HandleEnableTrigger2(const string& d, const RPCInfo& rpc){
	EnableTrigger req;
	req.ParseFromString(d);
	TableRegistry::Get()->tables()[req.table()]->trigger(req.trigger_id())->enable(req.enable());
	sendReply(rpc);
}

void Worker::HandleTermNotification2(const string& d, const RPCInfo& rpc){
	TerminationNotification req;
	req.ParseFromString(d);
	GlobalTableBase *ta = TableRegistry::Get()->table(0);	//we have only 1 table, index 0
	DLOG(INFO)<<"worker "<<id()<<" get a termination notification.";
	for(int i = 0; i < ta->num_shards(); ++i){
		if(ta->is_local_shard(i)){
			ta->get_partition(i)->terminate();
		}
	}
	sendReply(rpc);
}

void Worker::HandleRunKernel2(const std::string& d, const RPCInfo& rpc){
	kreq.ParseFromString(d);
	running_kernel_=true;
	sendReply(rpc);
	runKernel();
	finishKernel();
}

void Worker::HandleShutdown2(const string& , const RPCInfo& rpc){
	if(config_.master_id()==rpc.source){
		VLOG(1) << "Shutting down worker " << config_.worker_id();
		running_ = false;
	}else{
		LOG(INFO)<<"Ignore shutdown notification from "<<rpc.source<<" (only master's is accepted)";
	}
	sendReply(rpc);
}
void Worker::HandleReply(const std::string& d, const RPCInfo& rpc){
	ReplyMessage rm;
	rm.ParseFromString(d);
	int tag=rm.type();
	VLOG(2) << "Processing reply, type " << tag << ", from " << rpc.source << ", to " << rpc.dest;
	//TODO: add a reply buffer for sync checking i.e. NetworkThread::WaitForSync()
//	lock_guard<recursive_mutex> sl(rep_lock[tag]);
//	reply_buffer[tag][source].push_back(data);
}

} //namespace dsm
