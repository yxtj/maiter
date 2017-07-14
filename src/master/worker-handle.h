/*
 * worker-handle.h
 *
 *  Created on: Dec 9, 2015
 *      Author: tzhou
 */

#ifndef MASTER_WORKER_HANDLE_H_
#define MASTER_WORKER_HANDLE_H_

#include "run-descriptor.h"
#include "util/noncopyable.h"
#include "util/timer.h"
#include <glog/logging.h>

#include <vector>
#include <set>
#include <map>

namespace dsm {

struct Taskid{
	int table;
	int shard;

	Taskid(int t, int s) :
			table(t), shard(s){
	}

	bool operator<(const Taskid& b) const{
		return table < b.table || (table == b.table && shard < b.shard);
	}
};

struct TaskState: private noncopyable{
	enum Status{
		PENDING = 0, ACTIVE = 1, FINISHED = 2
	};

	TaskState(Taskid id, int64_t size) :
			id(id), status(PENDING), size(size), stolen(false){
	}

	static bool IdCompare(TaskState *a, TaskState *b){
		return a->id < b->id;
	}

	static bool WeightCompare(TaskState *a, TaskState *b){
		if(a->stolen && !b->stolen){
			return true;
		}
		return a->size < b->size;
	}

	Taskid id;
	int status;
	int size;
	bool stolen;
};

typedef std::map<Taskid, TaskState*> TaskMap;
typedef std::set<Taskid> ShardSet;

struct WorkerState: private noncopyable{
	WorkerState(int w_id) :
			id(w_id){
		net_id=-1;
		last_ping_time = Now();
		is_alive=true;
		last_task_start = 0.0;
		total_runtime = 0.0;
		checkpointing = false;
		receives = 0;
		updates = 0;
		current = 0;
		ndefault = 0;
	}

	TaskMap work;

	// Table shards this worker is responsible for serving.
	ShardSet shards;

	double last_ping_time;

	int status;
	int id;
	int net_id;
	bool is_alive;

	double last_task_start;
	double total_runtime;

	bool checkpointing;

	uint64_t receives;
	uint64_t updates;

	double current;
	uint64_t ndefault;

	// Order by number of pending tasks and last update time.
	static bool PendingCompare(WorkerState *a, WorkerState* b){
//    return (a->pending_size() < b->pending_size());
		return a->num_pending() < b->num_pending();
	}

	bool alive() const{
//		return dead_workers.find(id) == dead_workers.end();
		return is_alive;
	}

	bool is_assigned(Taskid id){
		return work.find(id) != work.end();
	}

	void ping(){
		last_ping_time = Now();
	}

	double idle_time(){
		// Wait a little while before stealing work; should really be
		// using something like the standard deviation, but this works
		// for now.
		if(num_finished() != work.size()) return 0;

		return Now() - last_ping_time;
	}

	// if should_service=true, assign shard
	// if should_service=false, unassign shard
	void assign_shard(int shard, bool should_service);

	bool serves(Taskid id) const{
		return shards.find(id) != shards.end();
	}

	void assign_task(TaskState *s){
		work[s->id] = s;
	}

	void remove_task(TaskState* s){
		work.erase(work.find(s->id));
	}

	void clear_tasks(){
		work.clear();
	}

	void set_finished(const Taskid& id){
		CHECK(work.find(id) != work.end());
		TaskState *t = work[id];
		CHECK(t->status == TaskState::ACTIVE);
		t->status = TaskState::FINISHED;
	}

#define COUNT_TASKS(name, type)\
  int num_ ## name() const {\
    int c = 0;\
    for (TaskMap::const_iterator i = work.begin(); i != work.end(); ++i)\
      if (i->second->status == TaskState::type) { ++c; }\
    return c;\
  }\
  int64_t name ## _size() const {\
      int64_t c = 0;\
      for (TaskMap::const_iterator i = work.begin(); i != work.end(); ++i)\
        if (i->second->status == TaskState::type) { c += i->second->size; }\
      return c;\
  }\
  std::vector<TaskState*> name() const {\
    std::vector<TaskState*> out;\
    for (TaskMap::const_iterator i = work.begin(); i != work.end(); ++i)\
      if (i->second->status == TaskState::type) { out.push_back(i->second); }\
    return out;\
  }

	COUNT_TASKS(pending, PENDING)
	COUNT_TASKS(active, ACTIVE)
	COUNT_TASKS(finished, FINISHED)
#undef COUNT_TASKS
//	int num_pending() const;
//	int num_active() const;
//	int num_finished() const;
//	int64_t pending_size() const;
//	int64_t active_size() const;
//	int64_t finished_size() const;
//	std::vector<TaskState*> pending() const;
//	std::vector<TaskState*> active() const;
//	std::vector<TaskState*> finished() const;

	int num_assigned() const{
		return work.size();
	}
	int64_t total_size() const{
		int64_t out = 0;
		for(TaskMap::const_iterator i = work.begin(); i != work.end(); ++i){
			out += 1 + i->second->size;
		}
		return out;
	}

	// Order pending tasks by our guess of how large they are
	bool get_next(const RunDescriptor& r, KernelRequest* msg);
};

} /* namespace dsm */

#endif /* MASTER_WORKER_HANDLE_H_ */
