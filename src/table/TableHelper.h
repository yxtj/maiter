/*
 * TableHelper.h
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TABLEHELPER_H_
#define TABLE_TABLEHELPER_H_

#include "worker/worker.pb.h"

namespace dsm{

// This interface is used by global tables to communicate with the outside
// world and determine the current state of a computation.
struct TableHelper{
	virtual int id() const = 0;
	virtual int epoch() const = 0;
	virtual int peer_for_shard(int table, int shard) const = 0;

	virtual void SendPutRequest(int dstWorkerID, const KVPairData& put) = 0;
	virtual void HandlePutRequest() = 0;
	virtual void FlushUpdates() = 0;

	virtual void SyncSwapRequest(const SwapTable& req) = 0;

	virtual void SyncClearRequest(const ClearTable& req) = 0;

	virtual void SendTermcheck(int index, long updates, double current) = 0;
	virtual ~TableHelper(){}
};

}

#endif /* TABLE_TABLEHELPER_H_ */
