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
#include <functional>

//DEFINE_string(dead_workers, "",
//		"For failure testing; comma delimited list of workers to pretend have died.");
DEFINE_bool(work_stealing, true, "Enable work stealing to load-balance tasks between machines.");
DEFINE_bool(checkpoint, false, "If true, enable checkpointing.");
DEFINE_bool(restore, false, "If true, enable restore.");
DEFINE_int32(termcheck_interval, 1, "");
DEFINE_string(track_log, "track_log", "");
DEFINE_bool(sync_track, false, "");

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);
DECLARE_double(sleep_time);

using namespace std;

namespace dsm {

//Helper
static void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}
void Master::addReplyHandler(const int mtype, void (Master::*fp)(), const bool newThread){
	rph_.addType(mtype,
		ReplyHandler::condFactory(ReplyHandler::EACH_ONE, config_.num_workers()),
		bind(fp,this),newThread);
}
//static unordered_set<int> dead_workers;


Master::Master(const ConfigData &conf) :
		tables_(TableRegistry::Get()->tables()){
	config_.CopyFrom(conf);
	checkpoint_epoch_ = 0;
	termcheck_epoch_ = 0;
	kernel_epoch_ = 0;
	finished_ = dispatched_ = 0;
	last_checkpoint_ = Now();
	last_termcheck_ = Now();
	checkpointing_ = false;
	terminated_ = false;
	network_ = NetworkThread2::Get();
	shards_assigned_ = false;
	conv_track_log.open(FLAGS_track_log.c_str());
	if(FLAGS_sync_track){
		sync_track_log.open("sync_track_log");
		iter = 0;
	}

	CHECK_GT(network_->size(), 1)<< "At least one master and one worker required!";
	registerHandlers();

	thread t(bind(&Master::MsgLoop,this));
	t.detach();

	for(int i = 0; i < config_.num_workers(); ++i){
		workers_.push_back(new WorkerState(i));
	}

	registerWorkers();

	LOG(INFO)<< "All workers registered; starting up.";

//	vector<StringPiece> bits = StringPiece::split(FLAGS_dead_workers, ",");
////  LOG(INFO) << "dead workers: " << FLAGS_dead_workers;
//	for(int i = 0; i < bits.size(); ++i){
//		LOG(INFO)<< MP(i, bits[i].AsString());
//		dead_workers.insert(strtod(bits[i].AsString().c_str(), NULL));
//	}
}

Master::~Master(){
	conv_track_log.close();
	if(FLAGS_sync_track) sync_track_log.close();
	LOG(INFO)<< "Total runtime: " << runtime_.elapsed();

	ostringstream buff;
	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		if(i!=0 && i % 10 == 0){
			buff<<"\n"<<setw(2)<<i<<": ";
		}
		buff<<setprecision(3)<<w.total_runtime<<" ";
	}
	LOG(INFO)<< "Worker execution time:\n"<<buff.str();

	LOG(INFO)<< "Kernel stats: ";
	for(MethodStatsMap::iterator i = method_stats_.begin(); i != method_stats_.end(); ++i){
		LOG(INFO)<< i->first << "--> " << i->second.ShortDebugString();
	}

	LOG(INFO)<< "Shutting down workers.";
//	EmptyMessage msg;
//	for(int i = 1; i < network_->size(); ++i){
//		network_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
//	}
	shutdownWorkers();
}

void Master::MsgLoop(){
	string data;
	RPCInfo info;
	info.dest=network_->id();
	while(!terminated_){
		if(network_->TryReadAny(data,&info.source,&info.tag)){
			driver_.pushData(data,info);
		}
		while(driver_.empty()){
			driver_.popData();
		}
		Sleep();
	}
}

void Master::terminate_iteration(){
	for(int i = 0; i < workers_.size(); ++i){
		int worker_id = i;
		TerminationNotification req;
		req.set_epoch(0);
		network_->Send(1 + worker_id, MTYPE_TERMINATION, req);
	}
	terminated_ = true;
	VLOG(1) << "Sent termination notifications ";
}

bool Master::termcheck(){
	//check_1 (pre-condition), all workers have finished and sent partial termcheck on their own parts.
	//check_2, using TermChecker
	VLOG(2) << "Starting termination check: " << termcheck_epoch_;

	Timer cp_timer;

	//TODO: Move this check_1 part into a separate function.
	//TODO: Optimize it by callback and condition variable.
	int received_num = 0;
	for(int i = 0; i < workers_.size(); ++i){
		int worker_id = i;
		VLOG(1) << "Starting termination checking on: " << worker_id;

		TermcheckDelta resp;
//		while(network_->TryRead(1 + worker_id, MTYPE_TERMCHECK_DONE, &resp)){ //read all the buffered information
//			VLOG(1) << "receive from " << worker_id << " with " << resp.delta();
//			workers_[worker_id]->current = resp.delta();
//			workers_[worker_id]->updates = resp.updates();
//			workers_[worker_id]->termchecking = true;
//		}
		if(workers_[worker_id]->termchecking==true)
			received_num++;
	}
	VLOG(1) << "received " << received_num << " size " << workers_.size();
	//if receive too less, ignore this termination check, need smaller snapshot_interval and longer termcheck_interval
	if(received_num < workers_.size())
		return false;

	long total_updates = 0;
	vector<double> partials;
	for(int i = 0; i < workers_.size(); ++i){
		total_updates += workers_[i]->updates;
		partials.push_back(workers_[i]->current);
	}

	//we only have one table
	bool bterm = ((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->terminate(partials);

	VLOG(0) << "Termination check at " << barrier_timer->elapsed() << " finished in "
						<< cp_timer.elapsed() << " total current "
						<< StringPrintf("%.05f",
								((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->get_curr())
						<< " total updates " << total_updates << endl;
	conv_track_log << "Termination check at " << barrier_timer->elapsed() << " finished in "
			<< cp_timer.elapsed() << " total current "
			<< StringPrintf("%.05f",
					((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->get_curr())
			<< " total updates " << total_updates << "\n";
	conv_track_log.flush();

	if(bterm){        //for pagerank termination
		return true;
	}else{	//reset
		for(int i = 0; i < workers_.size(); ++i){
			int worker_id = i;
			workers_[worker_id]->termchecking = false;
//
//			//clear buffer
//			TermcheckDelta resp;
//			while(network_->TryRead(1 + worker_id, MTYPE_TERMCHECK_DONE, &resp)){
//				VLOG(1)<<"clear termcheck message from "<<worker_id<<" with "<<resp.delta();
//			}
		}
		return false;
	}
}

void Master::termcheck2(){
	while(!terminated_){
		VLOG(2) << "Starting termination check: " << termcheck_epoch_;
		Timer cp_timer;

		su_term.wait();

		long total_updates = 0;
		vector<double> partials;
		for(int i = 0; i < workers_.size(); ++i){
			total_updates += workers_[i]->updates;
			partials.push_back(workers_[i]->current);
		}

		//we only have one table
		bool bterm = ((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->terminate(partials);

		LOG(INFO) << "Termination check at " << barrier_timer->elapsed() << " finished in "
				<< cp_timer.elapsed() << " total current "<< StringPrintf("%.05f",
						((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->get_curr())
				<< " total updates " << total_updates;
		conv_track_log << "Termination check at " << barrier_timer->elapsed() << " finished in "
				<< cp_timer.elapsed() << " total current "<< StringPrintf("%.05f",
						((TermChecker<int, double>*)(tables_[0]->info_.termchecker))->get_curr())
				<< " total updates " << total_updates << "\n";
		conv_track_log.flush();

	}
}

void Master::run_all(RunDescriptor r){
	run_range(r, range(r.table->num_shards()));
}

void Master::run_one(RunDescriptor r){
	run_range(r, range(1));
}

void Master::run_range(RunDescriptor r, const vector<int>& shards){
	r.shards = shards;
	run(r);
}

WorkerState* Master::worker_for_shard(int table, int shard){
	for(int i = 0; i < workers_.size(); ++i){
		if(workers_[i]->serves(Taskid(table, shard))){
			return workers_[i];
		}
	}

	return nullptr;
}

WorkerState* Master::assign_worker(int table, int shard){
	WorkerState* ws = worker_for_shard(table, shard);
	int64_t work_size = tables_[table]->shard_size(shard);

	if(ws){
//    LOG(INFO) << "Worker for shard: " << MP(table, shard, ws->id);
		ws->assign_task(new TaskState(Taskid(table, shard), work_size));
		return ws;
	}

	WorkerState* best = NULL;
	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		if(w.alive() && (best == NULL || w.shards.size() < best->shards.size())){
			best = workers_[i];
		}
	}

	CHECK(best != NULL) << "Ran out of workers!  Increase the number of partitions per worker!";

//  LOG(INFO) << "Assigned " << MP(table, shard, best->id);
	CHECK(best->alive());

	VLOG(1) << "Assigning " << MP(table, shard) << " to " << best->id;
	best->assign_shard(shard, true);
	best->assign_task(new TaskState(Taskid(table, shard), work_size));
	return best;
}

bool Master::steal_work(const RunDescriptor& r, int idle_worker, double avg_completion_time){
	if(!FLAGS_work_stealing){
		return false;
	}

	WorkerState &dst = *workers_[idle_worker];

	if(!dst.alive()){
		return false;
	}

	// Find the worker with the largest number of pending tasks.
	WorkerState& src = **max_element(workers_.begin(), workers_.end(),
			&WorkerState::PendingCompare);
	if(src.num_pending() == 0){
		return false;
	}

	vector<TaskState*> pending = src.pending();

	TaskState *task = *max_element(pending.begin(), pending.end(), TaskState::WeightCompare);
	if(task->stolen){
		return false;
	}

	double average_size = 0;

	for(int i = 0; i < r.table->num_shards(); ++i){
		average_size += r.table->shard_size(i);
	}
	average_size /= r.table->num_shards();

	// Weight the cost of moving the table versus the time savings.
	double move_cost = max(1.0, 2 * task->size * avg_completion_time / average_size);
	double eta = 0;
	for(int i = 0; i < pending.size(); ++i){
		TaskState *p = pending[i];
		eta += max(1.0, p->size * avg_completion_time / average_size);
	}

//  LOG(INFO) << "ETA: " << eta << " move cost: " << move_cost;

	if(eta <= move_cost){
		return false;
	}

	const Taskid& tid = task->id;
	task->stolen = true;

	LOG(INFO)<< "Worker " << idle_worker << " is stealing task "
			<< MP(tid.shard, task->size) << " from worker " << src.id;
	dst.assign_shard(tid.shard, true);
	src.assign_shard(tid.shard, false);

	src.remove_task(task);
	dst.assign_task(task);
	return true;
}

void Master::assign_tables(){
	shards_assigned_ = true;

	// Assign workers for all table shards, to ensure every shard has an owner.
	TableRegistry::Map &tables = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tables.begin(); i != tables.end(); ++i){
		for(int j = 0; j < i->second->num_shards(); ++j){
			assign_worker(i->first, j);
		}
	}
}

void Master::assign_tasks(const RunDescriptor& r, vector<int> shards){
	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		w.clear_tasks(); //XXX: did not delete task state, memory leak
	}

	for(int i = 0; i < shards.size(); ++i){
		assign_worker(r.table->id(), shards[i]);
	}
}

int Master::dispatch_work(const RunDescriptor& r){
	int num_dispatched = 0;
	KernelRequest w_req;
	for(int i = 0; i < workers_.size(); ++i){
		WorkerState& w = *workers_[i];
		if(w.num_pending() > 0 && w.num_active() == 0){
			w.get_next(r, &w_req);
			Args* p = r.params.ToMessage();
			w_req.mutable_args()->CopyFrom(*p);
			delete p;
			num_dispatched++;
			network_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
		}
	}
	return num_dispatched;
}

void Master::dump_stats(){
	string status;
	for(int k = 0; k < config_.num_workers(); ++k){
		status += StringPrintf("%d/%d ", workers_[k]->num_finished(), workers_[k]->num_assigned());
	}
	//LOG(INFO) << StringPrintf("Running %s (%d); %s; assigned: %d done: %d",
	//current_run_.method.c_str(), current_run_.shards.size(),
	//status.c_str(), dispatched_, finished_);

}

int Master::reap_one_task(){
	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];
	KernelDone done_msg;
	int w_id = 0;

//	if(network_->TryRead(Task::ANY_SRC, MTYPE_KERNEL_DONE, &done_msg, &w_id)){
	if(1){

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
		Sleep();
		return -1;
	}
}

void Master::run(RunDescriptor r){
	if(!FLAGS_checkpoint && r.checkpoint_type != CP_NONE){
		LOG(INFO)<< "Checkpoint is disabled by flag.";
		r.checkpoint_type = CP_NONE;
	}

	// HACKHACKHACK - register ourselves with any existing tables
	for (TableRegistry::Map::iterator i = tables_.begin(); i != tables_.end(); ++i){
		i->second->set_helper(this);
	}

	CHECK_EQ(current_run_.shards.size(), finished_) << " Cannot start kernel before previous one is finished ";
	finished_ = dispatched_ = 0;

	KernelInfo *k = KernelRegistry::Get()->kernel(r.kernel);
	CHECK_NE(r.table, (void*)NULL) << "Table locality must be specified!";
	CHECK_NE(k, (void*)NULL) << "Invalid kernel class " << r.kernel;
	CHECK_EQ(k->has_method(r.method), true) << "Invalid method: " << MP(r.kernel, r.method);

	VLOG(1) << "Running: " << r.kernel << " : " << r.method << " : " << *r.params.ToMessage();

	vector<int> shards = r.shards;

	MethodStats &mstats = method_stats_[r.kernel + ":" + r.method];
	mstats.set_calls(mstats.calls() + 1);

	// Fill in the list of tables to checkpoint, if it was left empty.
	if (r.checkpoint_tables.empty()){
		for (TableRegistry::Map::iterator i = tables_.begin(); i != tables_.end(); ++i){
			r.checkpoint_tables.push_back(i->first);
		}
	}

	current_run_ = r;
	current_run_start_ = Now();

	if (!shards_assigned_){
		//only perform table assignment before the first kernel run
		assign_tables();
		send_table_assignments();
	}

	kernel_epoch_++;

	assign_tasks(current_run_, shards);

	dispatched_ = dispatch_work(current_run_);

	thread t_cp;
	if(current_run_.checkpoint_type == CP_ROLLING){
		t_cp=thread(&Master::checkpoint,this);
	}
	thread t_term(&Master::termcheck2, this);

	barrier();

	finishKernel();
	t_cp.join();
	t_term.join();
}

void Master::barrier(){
//	MethodStats &mstats = method_stats_[current_run_.kernel + ":" + current_run_.method];

//	bool bterm = false;
	barrier_timer = new Timer();
	iter++;

	//VLOG(1) << "finished " << finished_ << " current_run " << current_run_.shards.size();
//	while(finished_ < current_run_.shards.size()){
//		//VLOG(1) << "finished " << finished_ << "current_rund " << current_run_.shards.size();
//		PERIODIC(10, {
//			DumpProfile();
//			dump_stats();
//		});
//
//		if(!terminated_ && Now() - last_termcheck_ > FLAGS_termcheck_interval){
//			bterm = termcheck();
//			last_termcheck_ = Now();
//			VLOG(2) << "term ? " << bterm;
//
//			if(bterm){
//				terminate_iteration();
//				terminated_ = true;
//			}
//		}
//
////		if(reap_one_task() >= 0){
////			finished_++;
////		...
//	}
	su_kerdone.wait();
	terminated_ = true;

	delete barrier_timer;
}

static void TestTaskSort(){
	vector<TaskState*> t;
	for(int i = 0; i < 100; ++i){
		t.push_back(new TaskState(Taskid(0, i), rand()));
	}

	sort(t.begin(), t.end(), &TaskState::WeightCompare);
	for(int i = 1; i < 100; ++i){
		CHECK_LE(t[i - 1]->size, t[i]->size);
	}
}

REGISTER_TEST(TaskSort, TestTaskSort());
}
