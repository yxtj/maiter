/*
 * TableHelper.h
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TABLEHELPER_H_
#define TABLE_TABLEHELPER_H_

#include <string>

//#include "msg/message.pb.h"

namespace dsm{

class KVPairData;
class InNeighborData;
class ValueRequest;

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
	virtual void realSendRequest(int dstWorkerID, const ValueRequest& req) = 0;
//	virtual void HandlePutRequest() = 0;
//	virtual void FlushUpdates() = 0;
	virtual void realSendTermCheck(int index, uint64_t receives, uint64_t updates, double current, uint64_t ndefault) = 0;

	virtual void realSendInNeighbor(int dstWorkerID, const InNeighborData& data) = 0;

	virtual void realSwap(const int tid1, const int tid2) = 0;
	virtual void realClear(const int tid) = 0;

	std::string genCPNameFolderPart(int taskid){
		return "task-" + std::to_string(taskid);
	}
	std::string genCPNameFolderPart(int taskid, int epoch){
		return "task-" + std::to_string(taskid) + "/epoch_" + std::to_string(epoch);
	}
	std::string genCPNameFilePart(int table, int shard){
		return "T" + std::to_string(table) + "-S" + std::to_string(shard);
	}

	virtual ~TableHelper(){}
};

}

#endif /* TABLE_TABLEHELPER_H_ */
