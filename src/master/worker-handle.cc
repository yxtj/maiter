/*
 * worker-handle.cc
 *
 *  Created on: Dec 9, 2015
 *      Author: tzhou
 */

#include <worker-handle.h>

using namespace std;

namespace dsm {

// if should_service=true, assign shard
// if should_service=false, unassign shard
void WorkerState::assign_shard(int shard, bool should_service){
	TableRegistry::Map &tables = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tables.begin(); i != tables.end(); ++i){
		if(shard < i->second->num_shards()){
			Taskid t(i->first, shard);
			if(should_service){
				shards.insert(t);
			}else{
				shards.erase(shards.find(t));
			}
		}
	}
}

// Order pending tasks by our guess of how large they are
bool WorkerState::get_next(const RunDescriptor& r, KernelRequest* msg){
	vector<TaskState*> p = pending();

	if(p.empty()){
		return false;
	}

	TaskState* best = *max_element(p.begin(), p.end(), &TaskState::WeightCompare);

	msg->set_kernel(r.kernel);
	msg->set_method(r.method);
	msg->set_table(r.table->id());
	msg->set_shard(best->id.shard);

	best->status = TaskState::ACTIVE;
	last_task_start = Now();

	return true;
}

} /* namespace dsm */
