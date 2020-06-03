/*
 * run-descriptor.cc
 *
 *  Created on: Dec 28, 2015
 *      Author: tzhou
 */

#include "run-descriptor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_string(checkpoint_type);
DECLARE_double(checkpoint_interval);
DECLARE_int32(checkpoint_restore_from);

namespace dsm{

void RunDescriptor::Init(const std::string& kernel,
		const std::string& method,
		GlobalTableBase *table,
		const bool checkpoint,
		const bool termcheck,
		const bool restore,
		const std::vector<int>& cp_tables)
{
	barrier = true;
	checkpoint_type = CP_NONE;
	if(checkpoint){
		if(checkpoint && !FLAGS_checkpoint_type.empty() &&
				!CheckpointType_Parse(FLAGS_checkpoint_type,&checkpoint_type)){
			LOG(FATAL)<<"Cannot understand given checkpoint type: "<<FLAGS_checkpoint_type;
		}
		checkpoint_interval = FLAGS_checkpoint_interval;
	}
	if(checkpoint_type != CP_NONE && checkpoint_interval<=0){
		LOG(FATAL)<<"Checkpoint interval is not given or is not positive.";
	}

	checkpoint_tables = cp_tables;
//	if(!checkpoint_tables.empty()){
//		checkpoint_type = CP_SYNC;
//	}

	this->termcheck=termcheck;

	if(restore && FLAGS_checkpoint_restore_from>=0){
		this->restore=true;
		this->restore_from_epoch=FLAGS_checkpoint_restore_from;
	}else{
		this->restore=false;
	}

	this->kernel = kernel;
	this->method = method;
	this->table = table;

}

}
