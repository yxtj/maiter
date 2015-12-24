/*
 * master2.cc
 *
 *  Created on: Dec 21, 2015
 *      Author: tzhou
 */

#include "master.h"
#include "master/worker-handle.h"
#include "table/local-table.h"
#include "table/table.h"
#include "table/global-table.h"
//#include "net/NetworkThread.h"
#include "net/Task.h"

#include <set>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>
//#include <functional>

DECLARE_bool(restore);
DECLARE_int32(termcheck_interval);
DECLARE_string(track_log);
DECLARE_bool(sync_track);

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(sleep_time);

using namespace std;

namespace dsm {

void Master::shutdownWorkers(){
	EmptyMessage msg;
//	for(int i = 0; i < config_.num_workers(); ++i){
	for(int i = 1; i < network_->size(); ++i){
		network_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
	}
}
void Master::realSwap(const int tid1, const int tid2){
	SwapTable req;
	req.set_table_a(tid1);
	req.set_table_b(tid2);
	VLOG(2) << StringPrintf("Sending swap request (%d <--> %d)", req.table_a(), req.table_b());

	su_swap.reset();
	network_->Broadcast(MTYPE_SWAP_TABLE, req);
	su_swap.wait();
}
void Master::realClear(const int tid){
	ClearTable req;
	req.set_table(tid);
	VLOG(2) << StringPrintf("Sending clear request (%d)", req.table());

	su_clear.reset();
	network_->Broadcast(MTYPE_CLEAR_TABLE, req);
	su_clear.wait();
}

void Master::enable_trigger(const TriggerID triggerid, int table, bool enable){
	EnableTrigger trigreq;
	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		trigreq.set_trigger_id(triggerid);
		trigreq.set_table(table);
		trigreq.set_enable(enable);
		network_->Send(w.id + 1, MTYPE_ENABLE_TRIGGER, trigreq);
	}
}

bool Master::restore(){
	if(!FLAGS_restore){
		LOG(INFO)<< "Restore disabled by flag.";
		return false;
	}

	if (!shards_assigned_){
		assign_tables();
		send_table_assignments();
	}

	Timer t;
	vector<string> matches = File::MatchingFilenames(FLAGS_checkpoint_read_dir + "/*/checkpoint.finished");
	if (matches.empty()){
		return false;
	}

	// Glob returns results in sorted order, so our last checkpoint will be the last.
	const char* fname = matches.back().c_str();
	int epoch = -1;
	CHECK_EQ(sscanf(fname, (FLAGS_checkpoint_read_dir + "/epoch_%05d/checkpoint.finished").c_str(), &epoch),
			1) << "Unexpected filename: " << fname;

	LOG(INFO) << "Restoring from file: " << matches.back();

	RecordFile rf(matches.back(), "r");
	CheckpointInfo info;
	Args checkpoint_vars;
	Args params;
	CHECK(rf.read(&info));
	CHECK(rf.read(&params));
	CHECK(rf.read(&checkpoint_vars));

	// XXX - RJP need to figure out how to properly handle rolling checkpoints.
	current_run_.params.FromMessage(params);

	cp_vars_.FromMessage(checkpoint_vars);

	LOG(INFO) << "Restoring state from checkpoint " << MP(info.kernel_epoch(), info.checkpoint_epoch());

	kernel_epoch_ = info.kernel_epoch();
	checkpoint_epoch_ = info.checkpoint_epoch();

	StartRestore req;
	req.set_epoch(epoch);
	network_->Broadcast(MTYPE_RESTORE, req);
	su_restore.wait();

	LOG(INFO) << "Checkpoint restored in " << t.elapsed() << " seconds.";
	return true;
}

void Master::terminate_iteration(){
//	for(int i = 0; i < workers_.size(); ++i){
//		int worker_id = i;
//		TerminationNotification req;
//		req.set_epoch(0);
//		network_->Send(1 + worker_id, MTYPE_TERMINATION, req);
//	}
	TerminationNotification req;
	req.set_epoch(0);
	VLOG(1) << "Sent termination notifications ";
	network_->Broadcast(MTYPE_TERMINATION, req);
}

void Master::finishKernel(){
	EmptyMessage empty;
	//1st round-trip to make sure all workers have flushed everything
//	network_->SyncBroadcast(MTYPE_WORKER_FLUSH, empty);
	su_wflush.reset();
	network_->Broadcast(MTYPE_WORKER_FLUSH, empty);
	su_wflush.wait();

	//2nd round-trip to make sure all workers have applied all updates
	//XXX: incorrect if MPI does not guarantee remote delivery
//	network_->SyncBroadcast(MTYPE_WORKER_APPLY, empty);
	su_wapply.reset();
	network_->Broadcast(MTYPE_WORKER_APPLY, empty);
	su_wapply.wait();

	kernel_terminated_=true;

//	if(current_run_.checkpoint_type == CP_MASTER_CONTROLLED){
//		if(!checkpointing_){
//			start_checkpoint();
//		}
//		finish_checkpoint();
//	}
	cv_cp.notify_all();
	su_term.notify();

	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];
	mstats.set_total_time(mstats.total_time() + Now() - current_run_start_);
	LOG(INFO)<< "Kernel '" << current_run_.method << "' finished in " << Now() - current_run_start_;
}

void Master::send_table_assignments(){
	ShardAssignmentRequest req;
	DVLOG(1)<<"Send table assignment";

	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		for(ShardSet::iterator j = w.shards.begin(); j != w.shards.end(); ++j){
			ShardAssignment* s = req.add_assign();
			s->set_new_worker(i);
			s->set_table(j->table);
			s->set_shard(j->shard);
//      s->set_old_worker(-1);
		}
	}

	su_tassign.reset();
	network_->Broadcast(MTYPE_SHARD_ASSIGNMENT, req);
	DVLOG(1)<<"Wait for table assignment finish";
	su_tassign.wait();
	DVLOG(1)<<"table assignment finished";
}


} //namespace dsm
