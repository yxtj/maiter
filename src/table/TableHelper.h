/*
 * TableHelper.h
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TABLEHELPER_H_
#define TABLE_TABLEHELPER_H_

//#include "msg/message.pb.h"

namespace dsm{

class KVPairData;

// This interface is used by global tables to communicate with the outside
// world and determine the current state of a computation.
struct TableHelper{
	virtual int id() const = 0;
	virtual int epoch() const = 0;
//	virtual int ownerOfShard(int table, int shard) const = 0;

	virtual void signalToProcess() = 0;
	virtual void signalToSend() = 0;
	virtual void signalToTermCheck() = 0;

	virtual void signalToPnS(){
		signalToProcess();
		signalToSend();
	}

	virtual void realSendUpdates(int dstWorkerID, const KVPairData& put) = 0;
//	virtual void HandlePutRequest() = 0;
//	virtual void FlushUpdates() = 0;
	virtual void realSendTermCheck(int index, long updates, double current) = 0;

	virtual void realSwap(const int tid1, const int tid2) = 0;
	virtual void realClear(const int tid) = 0;

	virtual ~TableHelper(){}
};

}

#endif /* TABLE_TABLEHELPER_H_ */
