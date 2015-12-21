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
//#include "net/NetworkThread2.h"
#include "net/Task.h"

#include <set>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>

DECLARE_bool(restore);
DECLARE_int32(termcheck_interval);
DECLARE_string(track_log);
DECLARE_bool(sync_track);

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(sleep_time);

using namespace std;

namespace dsm {

void Master::registerWorkers(){
	for(int i = 0; i < config_.num_workers(); ++i){
		RegisterWorkerRequest req;
		int src = 0;
		network_->Read(Task::ANY_SRC, MTYPE_REGISTER_WORKER, &req, &src);
		VLOG(1) << "Registered worker " << src - 1 << "; " << config_.num_workers() - 1 - i
							<< " remaining.";
	}
}

void Master::shutdownWorkers(){
	EmptyMessage msg;
	for(int i = 1; i < network_->size(); ++i){
		network_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
	}
}


void Master::SyncSwapRequest(const SwapTable& req){
	network_->SyncBroadcast(MTYPE_SWAP_TABLE, req);
}
void Master::SyncClearRequest(const ClearTable& req){
	network_->SyncBroadcast(MTYPE_CLEAR_TABLE, req);
}


int Master::reap_one_task2(){
	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];
	KernelDone done_msg;
	int w_id = 0;

	if(network_->TryRead(Task::ANY_SRC, MTYPE_KERNEL_DONE, &done_msg, &w_id)){

		w_id -= 1;

		WorkerState& w = *workers_[w_id];

		Taskid task_id(done_msg.kernel().table(), done_msg.kernel().shard());
		//      TaskState* task = w.work[task_id];
		//
		//      LOG(INFO) << "TASK_FINISHED "
		//                << r.method << " "
		//                << task_id.table << " " << task_id.shard << " on "
		//                << w_id << " in "
		//                << Now() - w.last_task_start << " size "
		//                << task->size <<
		//                " worker " << w.total_size();

		for(int i = 0; i < done_msg.shards_size(); ++i){
			const ShardInfo &si = done_msg.shards(i);
			tables_[si.table()]->UpdatePartitions(si);
		}

		w.set_finished(task_id);

		w.total_runtime += Now() - w.last_task_start;

		if(FLAGS_sync_track){
			sync_track_log << "iter " << iter << " worker_id " << w_id << " iter_time "
					<< barrier_timer->elapsed() << " total_time " << w.total_runtime << "\n";
			sync_track_log.flush();
		}

		mstats.set_shard_time(mstats.shard_time() + Now() - w.last_task_start);
		mstats.set_shard_calls(mstats.shard_calls() + 1);
		w.ping();
		return w_id;
	}else{
		this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
		return -1;
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
	network_->SyncBroadcast(MTYPE_RESTORE, req);

	LOG(INFO) << "Checkpoint restored in " << t.elapsed() << " seconds.";
	return true;
}

void Master::finishKernel(){
	EmptyMessage empty;
	//1st round-trip to make sure all workers have flushed everything
	network_->SyncBroadcast(MTYPE_WORKER_FLUSH, empty);

	//2nd round-trip to make sure all workers have applied all updates
	//XXX: incorrect if MPI does not guarantee remote delivery
	network_->SyncBroadcast(MTYPE_WORKER_APPLY, empty);

	if(current_run_.checkpoint_type == CP_MASTER_CONTROLLED){
		if(!checkpointing_){
			start_checkpoint();
		}
		finish_checkpoint();
	}

	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];
	mstats.set_total_time(mstats.total_time() + Now() - current_run_start_);
	LOG(INFO)<< "Kernel '" << current_run_.method << "' finished in " << Now() - current_run_start_;
}

void Master::send_table_assignments(){
	ShardAssignmentRequest req;

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

	network_->SyncBroadcast(MTYPE_SHARD_ASSIGNMENT, req);
}


} //namespace dsm
