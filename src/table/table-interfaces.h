/*
 * table-interfaces.h
 *
 *  Created on: Feb 18, 2016
 *      Author: tzhou
 */

#ifndef TABLE_TABLE_INTERFACES_H_
#define TABLE_TABLE_INTERFACES_H_

#include "table.h"
#include "table-coder.h"

namespace dsm{


// Checkpoint and restoration.
class Checkpointable{
public:
	virtual void start_checkpoint(const std::string& f) = 0;
	virtual void write_message(const KVPairData& put) = 0;
	virtual void finish_checkpoint() = 0;
	virtual void restore(const std::string& f) = 0;
	virtual ~Checkpointable(){}
};

class Serializable{
public:
	virtual void deserializeFromFile(TableCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToFile(TableCoder* out) = 0;
	virtual ~Serializable(){}
};

class Transmittable{
public:
	virtual void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToNet(KVPairCoder* out) = 0;
	virtual ~Transmittable(){}
};

class Snapshottable{
public:
	virtual void serializeToSnapshot(const std::string& f, uint64_t* receives, uint64_t* updates, double* totalF2, uint64_t* defaultF2) = 0;
	virtual ~Snapshottable(){}
};


} //namespace dsm

#endif /* TABLE_TABLE_INTERFACES_H_ */
