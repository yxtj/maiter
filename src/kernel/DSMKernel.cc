/*
 * DSMKernel.cc
 *
 *  Created on: Dec 7, 2015
 *      Author: tzhou
 */

#include "DSMKernel.h"
#include "table/table-registry.h"
#include <glog/logging.h>

namespace dsm {

class Worker;
class GlobalTable;

void DSMKernel::initialize_internal(Worker* w, int table_id, int shard){
	w_ = w;
	table_id_ = table_id;
	shard_ = shard;
}

void DSMKernel::set_args(const MarshalledMap& args){
	args_ = args;
}

GlobalTableBase* DSMKernel::get_table(int id){
//	GlobalTable* t = (GlobalTable*)TableRegistry::Get()->table(id);
	GlobalTableBase* t = dynamic_cast<GlobalTableBase*>(TableRegistry::Get()->table(id));
	CHECK_NE(t, (void*)NULL);
	return t;
}

} /* namespace dsm */
