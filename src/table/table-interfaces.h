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
#include <fstream>

namespace dsm{


// Checkpoint and restoration.
class Checkpointable{
public:
	// IO for asynchronous checkpoint, local state and online messages
	virtual void start_checkpoint(const std::string& f) = 0;
	virtual void write_message(const KVPairData& put) = 0;
	virtual void finish_checkpoint() = 0;
	virtual void load_checkpoint(const std::string& f) = 0;

	// IO for general methods, local state and buffer data 
	virtual int64_t dump(const std::string& f, TableCoder* out = nullptr) = 0;
	virtual void restore(const std::string& f, TableCoder* in = nullptr) = 0;
	virtual ~Checkpointable() = default;
};

class Serializable{
public:
	virtual void deserializeFromFile(TableCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToFile(TableCoder* out) = 0;
	virtual void serializeStateToFile(TableCoder* out) = 0;
	virtual ~Serializable() = default;
};

class Transmittable{
public:
	virtual void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToNet(KVPairCoder* out) = 0;
	virtual ~Transmittable() = default;
};

class Snapshottable{
public:
	virtual void serializeToSnapshot(const std::string& f, int64_t* updates, double* totalF2) = 0;
	virtual ~Snapshottable() = default;
};


} //namespace dsm

#endif /* TABLE_TABLE_INTERFACES_H_ */
