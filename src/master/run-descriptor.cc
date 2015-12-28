/*
 * run-descriptor.cc
 *
 *  Created on: Dec 28, 2015
 *      Author: tzhou
 */

#include "run-descriptor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(checkpoint_type,"CP_NONE","Type of checkpoint mechanism");
DEFINE_double(checkpoint_interval,0.0,"Interval of taking checkpoint(in second)");


namespace dsm{

void RunDescriptor::Init(const std::string& kernel,
		const std::string& method,
		GlobalTableBase *table,
		const std::vector<int>& cp_tables)
{
	barrier = true;
	checkpoint_type = CP_NONE;
	if(!FLAGS_checkpoint_type.empty() &&
			!CheckpointType_Parse(FLAGS_checkpoint_type,&checkpoint_type)){
		LOG(ERROR)<<"Cannot understand given checkpoint type: "<<FLAGS_checkpoint_type;
	}
	checkpoint_interval = FLAGS_checkpoint_interval;
	if(checkpoint_type != CP_NONE && checkpoint_interval<=0){
		LOG(ERROR)<<"Checkpoint interval is not given or is not positive.";
	}
	checkpoint_tables = cp_tables;

	if(!checkpoint_tables.empty()){
//		checkpoint_type = CP_MASTER_CONTROLLED;
		checkpoint_type = CP_SYNC;
	}

	this->kernel = kernel;
	this->method = method;
	this->table = table;

}

}
