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
	double checkpoint_interval;

	// Tables to checkpoint.  If empty, commit all tables.
	std::vector<int> checkpoint_tables;
	std::vector<int> shards;

	int epoch;

	// Key-value map of arguments to pass to kernel functions
	MarshalledMap params;

	RunDescriptor(){
		Init("bogus", "bogus", nullptr, false);
	}

	RunDescriptor(const std::string& kernel,
			const std::string& method,
			GlobalTableBase *table,
			const bool checkpoint,
			std::vector<int> cp_tables = std::vector<int>()){
		Init(kernel, method, table, checkpoint, cp_tables);
	}

	void Init(const std::string& kernel,
			const std::string& method,
			GlobalTableBase *table,
			const bool checkpoint,
			const std::vector<int>& cp_tables = std::vector<int>());
};

}

#endif /* MASTER_RUN_DESCRIPTOR_H_ */
