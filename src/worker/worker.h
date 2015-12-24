#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "kernel/kernel.h"
#include "table/TableHelper.h"
#include "table/table.h"
#include "table/local-table.h"
#include "table/global-table.h"
#include "msg/message.pb.h"
#include "net/RPCInfo.h"
#include "driver/MsgDriver.h"

#include <mutex>

namespace dsm {

class NetworkThread2;

// If this node is the master, return false immediately.  Otherwise
// start a worker and exit when the computation is finished.
bool StartWorker(const ConfigData& conf);

class Worker: public TableHelper, private noncopyable{
	struct Stub;
public:
	Worker(const ConfigData &c);
	~Worker();

	void Run();

	void KernelLoop();
	void MsgLoop();
	Stats get_stats(){
		return stats_;
	}

	void CheckForMasterUpdates();
	void CheckNetwork();

	void HandleSwapRequest(const std::string& d, const RPCInfo& rpc);
	void HandleClearRequest(const std::string& d, const RPCInfo& rpc);

	void HandleShardAssignment(const std::string& d, const RPCInfo& rpc);

	void SendPutRequest(int dstWorkerID, const KVPairData& msg);
	void HandlePutRequest(const std::string& data, const RPCInfo& info);

	// Barrier: wait until all table data is transmitted.
	void HandleFlush(const std::string& d, const RPCInfo& rpc);
	void HandleApply(const std::string& d, const RPCInfo& rpc);

	// Enable or disable triggers
	void HandleEnableTrigger(const std::string& d, const RPCInfo& rpc);

	// terminate iteration
	void HandleTermNotification(const std::string& d, const RPCInfo& rpc);

	//my new handlers:
	void HandleRunKernel(const std::string& d, const RPCInfo& rpc);
	void HandleShutdown(const std::string& d, const RPCInfo& rpc);
	void HandleReply(const std::string& d, const RPCInfo& rpc);

	int ownerOfShard(int table_id, int shard) const;
	int id() const{
		return config_.worker_id();
	}
	int epoch() const{
		return epoch_;
	}

	int64_t pending_kernel_bytes() const;
	bool network_idle() const;

	bool has_incoming_data() const;

private:
	void registerHandlers();
	void registerWorker();

	void waitKernel();
	void runKernel();
	void finishKernel();

	void sendReply(const RPCInfo& rpc);

	void StartCheckpoint(int epoch, CheckpointType type);
	void FinishCheckpoint();
	void SendTermcheck(int index, long updates, double current);
	void Restore(int epoch);
	void UpdateEpoch(int peer, int peer_epoch);

	typedef void (Worker::*callback_t)(const string&, const RPCInfo&);
	void RegDSPImmediate(const int type, callback_t fp, bool spawnThread=false);
	void RegDSPProcess(const int type, callback_t fp, bool spawnThread=false);
	void RegDSPDefault(callback_t fp);

	mutable std::recursive_mutex state_lock_;

	// The current epoch this worker is running within.
	int epoch_;

	int num_peers_;
	bool running_;	//whether this worker is running

	bool running_kernel_;	//whether this kernel is running
	KernelRequest kreq;	//the kernel running row

	CheckpointType active_checkpoint_;
	typedef unordered_map<int, bool> CheckpointMap;
	CheckpointMap checkpoint_tables_;

	ConfigData config_;

	// The status of other workers.
	vector<Stub*> peers_;

	NetworkThread2 *network_;
	unordered_set<GlobalTableBase*> dirty_tables_;

	Stats stats_;

	MsgDriver driver;
};

}

#endif /* WORKER_H_ */
