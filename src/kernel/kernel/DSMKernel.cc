/*
 * DSMKernel.cc
 *
 *  Created on: Dec 7, 2015
 *      Author: tzhou
 */

#include <glog/logging.h>
#include "kernel/kernel/DSMKernel.h"
#include "kernel/table-registry.h"

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

GlobalTable* DSMKernel::get_table(int id){
//	GlobalTable* t = (GlobalTable*)TableRegistry::Get()->table(id);
	GlobalTable* t = dynamic_cast<GlobalTable*>(TableRegistry::Get()->table(id));
	CHECK_NE(t, (void*)NULL);
	return t;
}

} /* namespace dsm */
