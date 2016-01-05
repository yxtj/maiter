/*
 * master_checkpoint.cc
 *
 *  Created on: Dec 21, 2015
 *      Author: tzhou
 */

#include "master.h"
#include "master/worker-handle.h"
#include "table/local-table.h"
#include "table/table.h"
#include "table/global-table.h"
#include "net/NetworkThread.h"
#include "net/Task.h"
#include "util/file.h"

#include <set>
#include <thread>
#include <chrono>

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_bool(restore);

namespace dsm{

void Master::start_checkpoint(){
	if(checkpointing_){
		return;
	}
	LOG(INFO)<< "Starting new checkpoint: " << checkpoint_epoch_<<" at "<<runtime_.elapsed();

//	checkpoint_epoch_ += 1;
	checkpointing_ = true;

	File::Mkdirs(FLAGS_checkpoint_write_dir+StringPrintf("/epoch_%04d/", checkpoint_epoch_));

//	if(current_run_.checkpoint_type == CP_NONE){
//		current_run_.checkpoint_type = CP_MASTER_CONTROLLED;
//	}
	for(int i = 0; i < workers_.size(); ++i){
		start_worker_checkpoint(i, current_run_);
	}

	su_cp_start.wait();
	su_cp_start.reset();
}

void Master::start_worker_checkpoint(int worker_id, const RunDescriptor &r){
	if(workers_[worker_id]->checkpointing){
		return;
	}

	workers_[worker_id]->checkpointing = true;

	CheckpointRequest req;
	req.set_epoch(checkpoint_epoch_);

	for(int i = 0; i < r.checkpoint_tables.size(); ++i){
		req.add_table(r.checkpoint_tables[i]);
	}

	VLOG(1)<<"Send checkpoint to: " << worker_id<<" with "<<req.ShortDebugString();
	network_->Send(workers_[worker_id]->net_id, MTYPE_CHECKPOINT_START, req);
}

void Master::finish_checkpoint(){

	CheckpointRequest req;
	req.set_epoch(checkpoint_epoch_);
	for(int i = 0; i < workers_.size(); ++i){
		CHECK_EQ(workers_[i]->checkpointing, true);
//		network_->Send(workers_[worker_id]->net_id, MTYPE_CHECKPOINT_FINISH, req);
	}
	network_->Broadcast(MTYPE_CHECKPOINT_FINISH,req);

	su_cp_finish.wait();
	su_cp_finish.reset();

	for(int i = 0; i < workers_.size(); ++i){
		workers_[i]->checkpointing=false;
	}

	Args *params = current_run_.params.ToMessage();
	Args *cp_vars = cp_vars_.ToMessage();

	RecordFile rf(FLAGS_checkpoint_write_dir +
			StringPrintf("/epoch_%04d/checkpoint.finished", checkpoint_epoch_), "w");

	CheckpointInfo cinfo;
	cinfo.set_checkpoint_epoch(checkpoint_epoch_);
	cinfo.set_kernel_epoch(kernel_epoch_);

	rf.write(cinfo);
	rf.write(*params);
	rf.write(*cp_vars);
	rf.sync();

	delete params;
	delete cp_vars;
	checkpointing_ = false;
	last_checkpoint_ = Now();
}

//void Master::finish_worker_checkpoint(int worker_id){
//	CHECK_EQ(workers_[worker_id]->checkpointing, true);
//
//	CheckpointRequest req;
//	req.set_epoch(checkpoint_epoch_);
//	network_->Send(workers_[worker_id]->net_id, MTYPE_CHECKPOINT_FINISH, req);
//
//	VLOG(1) <<"Worker "<< worker_id << " finished checkpointing.";
//	workers_[worker_id]->checkpointing = false;
//}

void Master::checkpoint(){
	mutex m;
	chrono::duration<double> wt=chrono::duration<double>(current_run_.checkpoint_interval);
	unique_lock<mutex> ul(m);
//	auto pred=[&](){return Now()-last_checkpoint_>current_run_.checkpoint_interval;}
	cv_cp.wait_for(ul,wt);
	while(!kernel_terminated_){
		//TODO: add mechanism for existing and abandoning unfinished cp when the kernel is done.
		Timer cp_timer;
		start_checkpoint();
		su_cp_local.wait();
		su_cp_local.reset();
		finish_checkpoint();
		LOG(INFO)<< "Checkpoint "<<checkpoint_epoch_<<" finished in " << cp_timer.elapsed();
		checkpoint_epoch_++;
		cv_cp.wait_for(ul,wt);
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

	RestoreRequest req;
	req.set_epoch(epoch);
	network_->Broadcast(MTYPE_RESTORE, req);
	su_cp_restore.wait();

	LOG(INFO) << "Checkpoint restored in " << t.elapsed() << " seconds.";
	return true;
}


} //namespace dsm
