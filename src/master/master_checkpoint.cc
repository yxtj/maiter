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
#include "net/NetworkThread2.h"
#include "net/Task.h"

#include <set>
#include <thread>
#include <chrono>

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(sleep_time);

namespace dsm{

void Master::start_checkpoint(){
	if(checkpointing_){
		return;
	}
	LOG(INFO)<< "Starting new checkpoint: " << checkpoint_epoch_;

	Timer cp_timer;
	checkpoint_epoch_ += 1;
	checkpointing_ = true;

	File::Mkdirs(
			StringPrintf("%s/epoch_%05d/", FLAGS_checkpoint_write_dir.c_str(), checkpoint_epoch_));

	if(current_run_.checkpoint_type == CP_NONE){
		current_run_.checkpoint_type = CP_MASTER_CONTROLLED;
	}
	for(int i = 0; i < workers_.size(); ++i){
		start_worker_checkpoint(i, current_run_);
	}
	LOG(INFO)<< "Checkpoint finished in " << cp_timer.elapsed();
}

void Master::start_worker_checkpoint(int worker_id, const RunDescriptor &r){
	start_checkpoint();

	if(workers_[worker_id]->checkpointing){
		return;
	}

	VLOG(1) << "Starting checkpoint on: " << worker_id;

	workers_[worker_id]->checkpointing = true;

	CheckpointRequest req;
	req.set_epoch(checkpoint_epoch_);
	req.set_checkpoint_type(r.checkpoint_type);

	for(int i = 0; i < r.checkpoint_tables.size(); ++i){
		req.add_table(r.checkpoint_tables[i]);
	}

	network_->Send(1 + worker_id, MTYPE_START_CHECKPOINT, req);
}

void Master::finish_checkpoint(){
	for(int i = 0; i < workers_.size(); ++i){
		finish_worker_checkpoint(i, current_run_);
		CHECK_EQ(workers_[i]->checkpointing, false);
	}

	Args *params = current_run_.params.ToMessage();
	Args *cp_vars = cp_vars_.ToMessage();

	RecordFile rf(
			StringPrintf("%s/epoch_%05d/checkpoint.finished", FLAGS_checkpoint_write_dir.c_str(),
					checkpoint_epoch_), "w");

	CheckpointInfo cinfo;
	cinfo.set_checkpoint_epoch(checkpoint_epoch_);
	cinfo.set_kernel_epoch(kernel_epoch_);

	rf.write(cinfo);
	rf.write(*params);
	rf.write(*cp_vars);
	rf.sync();

	checkpointing_ = false;
	last_checkpoint_ = Now();
	delete params;
	delete cp_vars;
}

void Master::finish_worker_checkpoint(int worker_id, const RunDescriptor& r){
	CHECK_EQ(workers_[worker_id]->checkpointing, true);

	if(r.checkpoint_type == CP_MASTER_CONTROLLED){
		EmptyMessage req;
		network_->Send(1 + worker_id, MTYPE_FINISH_CHECKPOINT, req);
	}

	EmptyMessage resp;
//	network_->Read(1 + worker_id, MTYPE_CHECKPOINT_DONE, &resp);

	VLOG(1) << worker_id << " finished checkpointing.";
	workers_[worker_id]->checkpointing = false;
}

void Master::checkpoint(){
	mutex m;
	unique_lock<mutex> ul(m);
//	auto pred=[&](){return Now()-last_checkpoint_>current_run_.checkpoint_interval;}
	cv_cp.wait_for(ul,chrono::duration<double>(current_run_.checkpoint_interval));
	while(!terminated_){
		start_checkpoint();
		finish_checkpoint();
		cv_cp.wait_for(ul,chrono::duration<double>(current_run_.checkpoint_interval));
	}
}

} //namespace dsm
