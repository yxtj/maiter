/*
 * master_handler.cc
 *
 *  Created on: Dec 21, 2015
 *      Author: tzhou
 */

#include "master.h"
#include "master/worker-handle.h"
//#include "table/local-table.h"
//#include "table/global-table.h"
//#include "net/NetworkThread.h"
//#include "net/Task.h"

#include <gflags/gflags.h>
#include <string>
#include <thread>
#include <chrono>
#include <functional>

using namespace std;
using namespace std::placeholders;

namespace dsm{

//register helpers
void Master::RegDSPImmediate(const int type, callback_t fp){
	driver_.registerImmediateHandler(type, bind(fp, this, _1, _2));
}
void Master::RegDSPProcess(const int type, callback_t fp){
	driver_.registerProcessHandler(type, bind(fp, this, _1, _2));
}
void Master::RegDSPDefault(callback_t fp){
	driver_.registerDefaultOutHandler(bind(fp, this, _1, _2));
}
void Master::addReplyHandler(const int mtype, void (Master::*fp)(), const bool newThread){
	rph_.addType(mtype,
		ReplyHandler::condFactory(ReplyHandler::EACH_ONE, config_.num_workers()),
		bind(fp,this),newThread);
}

void Master::registerHandlers(){
	VLOG(1)<<"Master is registering handlers";
	//message handlers:
//	RegDSPImmediate(MTYPE_WORKER_REGISTER, &Master::handleRegisterWorker);
	RegDSPProcess(MTYPE_WORKER_REGISTER, &Master::handleRegisterWorker);
	RegDSPProcess(MTYPE_KERNEL_DONE, &Master::handleKernelDone);
	RegDSPProcess(MTYPE_TERMCHECK_LOCAL, &Master::handleTermcheckDone);
	RegDSPProcess(MTYPE_CHECKPOINT_LOCAL_DONE, &Master::handleCPLocalDone);

	//reply handlers:
	int nw=config_.num_workers();
	ReplyHandler::ConditionType EACH_ONE=ReplyHandler::EACH_ONE;
	RegDSPProcess(MTYPE_REPLY, &Master::handleReply);
	//type 1: called by handleReply() directly
	rph_.addType(MTYPE_WORKER_FLUSH, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_wflush));
	rph_.activateType(MTYPE_WORKER_FLUSH);
	rph_.addType(MTYPE_WORKER_APPLY, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_wapply));
	rph_.activateType(MTYPE_WORKER_APPLY);
	rph_.addType(MTYPE_SHARD_ASSIGNMENT, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_tassign));
	rph_.activateType(MTYPE_SHARD_ASSIGNMENT);
	rph_.addType(MTYPE_TABLE_CLEAR, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_clear));
	rph_.activateType(MTYPE_TABLE_CLEAR);
	rph_.addType(MTYPE_TABLE_SWAP, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_swap));
	rph_.activateType(MTYPE_TABLE_SWAP);

	rph_.addType(MTYPE_WORKER_LIST, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_regw));
	rph_.activateType(MTYPE_WORKER_LIST);

	rph_.addType(MTYPE_CHECKPOINT_START, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_cp_start));
	rph_.activateType(MTYPE_CHECKPOINT_START);
	rph_.addType(MTYPE_CHECKPOINT_LOCAL_DONE, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_cp_local));
	rph_.activateType(MTYPE_CHECKPOINT_LOCAL_DONE);
	rph_.addType(MTYPE_CHECKPOINT_FINISH, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_cp_finish));
	rph_.activateType(MTYPE_CHECKPOINT_FINISH);
	rph_.addType(MTYPE_RESTORE, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_cp_restore));
	rph_.activateType(MTYPE_RESTORE);


	//type 2: called by specific functions (handlers)
	// called by handlerRegisterWorker()
	rph_.addType(MTYPE_WORKER_REGISTER, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_regw));
	rph_.activateType(MTYPE_WORKER_REGISTER);
	// called by handleKernelDone()
	rph_.addType(MTYPE_KERNEL_DONE, ReplyHandler::condFactory(EACH_ONE,nw),
			bind(&SyncUnit::notify, &su_kerdone));
	rph_.activateType(MTYPE_KERNEL_DONE);
	// called by handleTermcheckDone()
	rph_.addType(MTYPE_TERMCHECK_LOCAL, ReplyHandler::condFactory(EACH_ONE,nw),
//			[&](){
//		VLOG(1)<<"termcheck done notified";
//		su_term.notify();
//	});
			bind(&SyncUnit::notify, &su_term));
	rph_.activateType(MTYPE_TERMCHECK_LOCAL);

}

void Master::handleReply(const std::string& d, const RPCInfo& info){
	ReplyMessage rep;
	rep.ParseFromString(d);
	DVLOG(2)<<"process reply from "<<info.source<<", type "<<rep.type();
	bool v=rph_.input(rep.type(),netId2worker_.at(info.source)->id);
	DVLOG(2)<<"process reply from "<<info.source<<", type "<<rep.type()<<", res "<<v;
}

void Master::handleRegisterWorker(const std::string& d, const RPCInfo& info){
	RegisterWorkerRequest req;
	req.ParseFromString(d);
	VLOG(1)<<"Registered worker: " << req.id();
	netId2worker_[info.source]=workers_[req.id()];
	workers_[req.id()]->net_id=info.source;
	rph_.input(MTYPE_WORKER_REGISTER, req.id());
}

void Master::handleKernelDone(const std::string& d, const RPCInfo& info){
	KernelDone req;
	req.ParseFromString(d);

	for(int i = 0; i < req.shards_size(); ++i){
		const ShardInfo &si = req.shards(i);
		tables_[si.table()]->UpdatePartitions(si);
	}

	int w_id = req.wid();

	VLOG(1)<<"Receive Kernel done from worker "<<w_id<< ": "<<req.kernel().kernel();

	Taskid task_id(req.kernel().table(), req.kernel().shard());
	WorkerState& w = *workers_[w_id];
	w.set_finished(task_id);
	w.total_runtime += Now() - w.last_task_start;

	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];
	mstats.set_shard_time(mstats.shard_time() + Now() - w.last_task_start);
	mstats.set_shard_calls(mstats.shard_calls() + 1);

	w.ping();

	//second part
	PERIODIC(0.1, {
		double avg_completion_time = mstats.shard_time() / mstats.shard_calls();
		bool need_update = false;
		for(int i = 0; i < workers_.size(); ++i){
			WorkerState& w = *workers_[i];
			// Don't try to steal tasks if the payoff is too small.
			if(mstats.shard_calls() > 10 && avg_completion_time > 0.2 && !checkpointing_
					&& w.idle_time() > 0.5){
				if(steal_work(current_run_, w.id, avg_completion_time)){
					need_update = true;
				}
			}

			if(current_run_.checkpoint_type == CP_SYNC
					&& 0.7 * current_run_.shards.size() < finished_ && w.idle_time() > 0
					&& !w.checkpointing){
				start_worker_checkpoint(w.id, current_run_);
			}
		}
		if(need_update){// Update the table assignments.
			send_table_assignments();
		}
	});

//	if(dispatched_ < current_run_.shards.size()){
//		dispatched_ += startWorkers(current_run_);
//	}

	finished_++;
	rph_.input(MTYPE_KERNEL_DONE, w_id);
}

void Master::handleTermcheckDone(const std::string& d, const RPCInfo& info){
	TermcheckDelta resp;
	resp.ParseFromString(d);
	int worker_id=resp.wid();
	VLOG(1) << "receive from " << resp.wid() << " with (" << resp.delta()<<" , "<<resp.ndefault()<<")";
	workers_[worker_id]->receives = resp.receives();
	workers_[worker_id]->updates = resp.updates();
	workers_[worker_id]->current = resp.delta();
	workers_[worker_id]->ndefault = resp.ndefault();

	rph_.input(MTYPE_TERMCHECK_LOCAL, resp.wid());
}

void Master::handleCPLocalDone(const std::string& d, const RPCInfo& info){
	CheckpointLocalDone resp;
	resp.ParseFromString(d);
	if(resp.epoch()!=checkpoint_epoch_){
		VLOG(2)<<"skip unmatched cp local done report";
		return;
	}
	rph_.input(MTYPE_CHECKPOINT_LOCAL_DONE,resp.wid());
}

//void Master::handleCheckpointDone(const std::string& d, const RPCInfo& info){
//	int worker_id=info.source-1;
//	su_cpdone[worker_id].notify();
//}

} //namespace dsm
