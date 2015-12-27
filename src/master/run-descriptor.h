/*
 * run-descriptor.h
 *
 *  Created on: Dec 9, 2015
 *      Author: tzhou
 */

#ifndef MASTER_RUN_DESCRIPTOR_H_
#define MASTER_RUN_DESCRIPTOR_H_

#include "msg/message.pb.h"
#include "util/marshalled_map.hpp"
#include <string>
#include <vector>

namespace dsm {

class GlobalTableBase;

struct RunDescriptor{
	std::string kernel;
	std::string method;

	GlobalTableBase *table;
	bool barrier;

	CheckpointType checkpoint_type;
	int checkpoint_interval;

	// Tables to checkpoint.  If empty, commit all tables.
	std::vector<int> checkpoint_tables;
	std::vector<int> shards;

	int epoch;

	// Key-value map of arguments to pass to kernel functions
	MarshalledMap params;

	RunDescriptor(){
		Init("bogus", "bogus", nullptr);
	}

	RunDescriptor(const std::string& kernel,
			const std::string& method,
			GlobalTableBase *table,
			std::vector<int> cp_tables = std::vector<int>()){
		Init(kernel, method, table, cp_tables);
	}

	void Init(const std::string& kernel,
			const std::string& method,
			GlobalTableBase *table,
			const std::vector<int>& cp_tables = std::vector<int>()){
		barrier = true;
//		checkpoint_type = CP_NONE;
		checkpoint_type = CP_MASTER_CONTROLLED
		checkpoint_interval = -1;
		checkpoint_tables = cp_tables;

		if(!checkpoint_tables.empty()){
			checkpoint_type = CP_MASTER_CONTROLLED;
		}

		this->kernel = kernel;
		this->method = method;
		this->table = table;
	}
};

}

#endif /* MASTER_RUN_DESCRIPTOR_H_ */
