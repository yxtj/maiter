/*
 * table_descriptor.h
 *
 *  Created on: Dec 4, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TABLE_DESCRIPTOR_H_
#define TABLE_TABLE_DESCRIPTOR_H_

#include <vector>

//#include "sharder.h"
//#include "term_checker.h"
//#include "trigger.h"
//#include "marshal.h"
//#include "kernel/kernel/IterateKernel.h"
//#include "TableHelper.h"

namespace dsm {

class TriggerBase;
class SharderBase;
class IterateKernelBase;
class TermCheckerBase;
class MarshalBase;
class TableFactory;
class TableHelper;

struct TableDescriptor{
public:
	TableDescriptor(){
		Reset();
	}

	TableDescriptor(int id, int shards){
		Reset();
		table_id = id;
		num_shards = shards;
	}

	void Reset(){
		table_id = -1;
		num_shards = -1;
		max_stale_time = 0.;
		helper = nullptr;
		partition_factory = nullptr;
		key_marshal = value1_marshal = value2_marshal = value3_marshal = nullptr;
		sharder = nullptr;
		iterkernel = nullptr;
		termchecker = nullptr;
	}

	int table_id;
	int num_shards;

	// For local tables, the shard of the global table they represent.
	int shard;
	int default_shard_size;
	double schedule_portion;

	std::vector<TriggerBase*> triggers;

	SharderBase *sharder;
	IterateKernelBase *iterkernel;
	TermCheckerBase *termchecker;

	MarshalBase *key_marshal;
	MarshalBase *value1_marshal;
	MarshalBase *value2_marshal;
	MarshalBase *value3_marshal;

	// For global tables, factory for constructing new partitions.
	TableFactory *partition_factory;
	TableFactory *deltaT_factory;

	// For global tables, the maximum amount of time to cache remote values
	double max_stale_time;

	// For global tables, reference to the local worker.  Used for passing
	// off remote access requests.
	TableHelper *helper;
};

} //namespace dsm

#endif /* TABLE_TABLE_DESCRIPTOR_H_ */
