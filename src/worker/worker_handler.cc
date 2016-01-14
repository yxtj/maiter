/*
 * worker_handler.cc
 *
 *  Created on: Dec 16, 2015
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
	RegDSPProcess(MTYPE_SHARD_ASSIGNMENT, &Worker::HandleShardAssignment);
	RegDSPProcess(MTYPE_CLEAR_TABLE, &Worker::HandleClearRequest);
	RegDSPProcess(MTYPE_SWAP_TABLE, &Worker::HandleSwapRequest);
	RegDSPProcess(MTYPE_WORKER_FLUSH, &Worker::HandleFlush);
	RegDSPProcess(MTYPE_WORKER_APPLY, &Worker::HandleApply);
	RegDSPProcess(MTYPE_ENABLE_TRIGGER, &Worker::HandleEnableTrigger);
	RegDSPProcess(MTYPE_TERMINATION, &Worker::HandleTermNotification);

	RegDSPProcess(MTYPE_RUN_KERNEL,&Worker::HandleRunKernel);
	RegDSPProcess(MTYPE_WORKER_SHUTDOWN, &Worker::HandleShutdown);
	RegDSPProcess(MTYPE_WORKER_LIST, &Worker::HandleWorkerList);
	RegDSPProcess(MTYPE_REPLY, &Worker::HandleReply);

	RegDSPProcess(MTYPE_PUT_REQUEST, &Worker::HandlePutRequest);

	RegDSPProcess(MTYPE_RESTORE, &Worker::HandleRestore);

	//Synchronization control:
	int nw=config_.num_workers();
	ReplyHandler::ConditionType EACH_ONE=ReplyHandler::EACH_ONE;
	rph.addType(MTYPE_CHECKPOINT_SIG,ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify,&su_cp_sig),false);
	rph.activateType(MTYPE_CHECKPOINT_SIG);
	return;
}

void Worker::HandleReply(const std::string& d, const RPCInfo& rpc){
	ReplyMessage rm;
	rm.ParseFromString(d);
	int tag=rm.type();
//	rph.input(tag,nid2wid[rpc.source]);
	DVLOG(2) << "Processing reply, type " << tag << ", from " << rpc.source << ", to " << rpc.dest;
}

void Worker::HandlePutRequest(const string& d, const RPCInfo& info){
	KVPairData put;
	put.ParseFromString(d);
	HandlePutRequestReal(put);
}


void Worker::HandleSwapRequest(const string& d, const RPCInfo& rpc){
	SwapTable req;
	req.ParseFromString(d);
	MutableGlobalTableBase *ta = TableRegistry::Get()->mutable_table(req.table_a());
	MutableGlobalTableBase *tb = TableRegistry::Get()->mutable_table(req.table_b());
	ta->local_swap(tb);

	sendReply(rpc);
}

void Worker::HandleClearRequest(const string& d, const RPCInfo& rpc){
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

void Worker::HandleShardAssignment(const string& d,const RPCInfo& rpc){
	ShardAssignmentRequest shard_req;
	shard_req.ParseFromString(d);
//  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
	for(int i = 0; i < shard_req.assign_size(); ++i){
		const ShardAssignmentRequest::ShardAssignment &a = shard_req.assign(i);
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
						<< " new owner is : " << id();
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

void Worker::HandleFlush(const string& d, const RPCInfo& rpc){
	Timer net;
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(t){
			t->SendUpdates();
		}
	}

	network_->Flush();
	stats_["flush_time"] += net.elapsed();
	sendReply(rpc);
}


void Worker::HandleApply(const string& d, const RPCInfo& rpc){
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(t){
			t->ProcessUpdates();
		}
	}
	sendReply(rpc);
}

void Worker::HandleEnableTrigger(const string& d, const RPCInfo& rpc){
	EnableTrigger req;
	req.ParseFromString(d);
	TableRegistry::Get()->tables()[req.table()]->trigger(req.trigger_id())->enable(req.enable());
	sendReply(rpc);
}

void Worker::HandleTermNotification(const string& d, const RPCInfo& rpc){
	TerminationNotification req;
	req.ParseFromString(d);
	GlobalTableBase *ta = TableRegistry::Get()->table(0);	//we have only 1 table, index 0
	DLOG(INFO)<<"worker "<<id()<<" get a termination notification.";
	for(int i = 0; i < ta->num_shards(); ++i){
		if(ta->is_local_shard(i)){
			ta->get_partition(i)->terminate();
		}
	}
//	clearUnprocessedPut();
	sendReply(rpc);
}

void Worker::HandleRunKernel(const std::string& d, const RPCInfo& rpc){
	kreq.ParseFromString(d);
	running_kernel_=true;
	sendReply(rpc);
	if(th_ker_)
		th_ker_->join();
	delete th_ker_;
	th_ker_=new thread(&Worker::KernelProcess, this);
}

void Worker::HandleShutdown(const string& , const RPCInfo& rpc){
	if(config_.master_id()==rpc.source){
		VLOG(1) << "Shutting down worker " << id();
		running_ = false;
	}else{
		LOG(INFO)<<"Ignore shutdown notification from "<<rpc.source<<" (only master's is accepted)";
	}
	sendReply(rpc);
}

void Worker::HandleWorkerList(const std::string& d, const RPCInfo& rpc){
	WorkerIDMapList req;
	req.ParseFromString(d);
	for(int i=0;i<req.workers_size();++i){
		const WorkerIDMapList::WorkerIDMap &p = req.workers(i);
		peers_[p.worker_id()].net_id=p.network_id();
		nid2wid[p.network_id()]=p.worker_id();
	}
	sendReply(rpc);
}

void Worker::HandleStartCheckpoint(const string& d, const RPCInfo& rpc){
	CheckpointRequest req;
	req.ParseFromString(d);
	//this handler may be run asynchronously, parameter may be invalid when used.
	bool res=startCheckpoint(req.epoch());
	sendReply(rpc,res);
}

void Worker::HandleFinishCheckpoint(const string& d, const RPCInfo& rpc){
	CheckpointRequest req;
	req.ParseFromString(d);
	bool res=finishCheckpoint(req.epoch());
	sendReply(rpc,res);
}

void Worker::HandleCheckpointSig(const string& d, const RPCInfo& rpc){
	CheckpointSyncSig req;
	req.ParseFromString(d);
	bool res=processCPSig(req.wid(),req.epoch());
	sendReply(rpc,res);
}

void Worker::HandleRestore(const string& d, const RPCInfo& rpc){
	RestoreRequest req;
	req.ParseFromString(d);
	restore(req.epoch());
	sendReply(rpc);
}

} //namespace dsm
