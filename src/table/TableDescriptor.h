/*
 * table_descriptor.h
 *
 *  Created on: Dec 4, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TABLEDESCRIPTOR_H_
#define TABLE_TABLEDESCRIPTOR_H_

#include <vector>

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
		init();
	}

	TableDescriptor(int id, int shards){
		init();
		table_id = id;
		num_shards = shards;
	}

	void init();
	void reset();

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

#endif /* TABLE_TABLEDESCRIPTOR_H_ */
