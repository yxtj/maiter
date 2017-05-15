#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "table/TableHelper.h"
#include "msg/message.pb.h"
#include "net/RPCInfo.h"
#include "driver/MsgDriver.h"
#include "driver/tools/ReplyHandler.h"
#include "driver/tools/SyncUnit.h"

#include <thread>
#include <mutex>

namespace dsm {

class NetworkThread;
class GlobalTableBase;

// If this node is the master, return false immediately.  Otherwise
// start a worker and exit when the computation is finished.
bool StartWorker(const ConfigData& conf);

class Worker: public TableHelper, private noncopyable{
public:
	Worker(const ConfigData &c);
	~Worker();

	void Run();

	void KernelProcess();
	void MsgLoop();

	virtual void realSwap(const int tid1, const int tid2);
	virtual void realClear(const int tid);
	void HandleSwapRequest(const std::string& d, const RPCInfo& rpc);
	void HandleClearRequest(const std::string& d, const RPCInfo& rpc);

	// step 0: evolving graph: coordinate in-neighbors
	void HandleAddInNeighbor(const std::string& d, const RPCInfo& rpc);
	virtual void realSendInNeighbor(int dstWorkerID, const InNeighborData& data);

	void HandleShardAssignment(const std::string& d, const RPCInfo& rpc);

	// step 1: get input PutRequest data and merge it into current local table
	void HandlePutRequest(const std::string& data, const RPCInfo& info);

	// step 2: process current local table, and store updates into their local mirror table
	virtual void signalToProcess();
	void HandleProcessUpdates(const std::string&, const RPCInfo&);	//dummy parameters

	// step 3: send out data in local mirrors
	virtual void signalToSend();
	void HandleSendUpdates(const std::string&, const RPCInfo&);	//dummy parameters
	virtual void realSendUpdates(int dstWorkerID, const KVPairData& msg);

	virtual void signalToPnS();	// process and send
	void HandlePnS(const std::string&, const RPCInfo&);	//dummy parameters

	// step 4: check whether current task is finished
	virtual void signalToTermCheck();
	void HandleTermCheck(const std::string&, const RPCInfo&);	//dummy parameters
	virtual void realSendTermCheck(int index, uint64_t updates, double current, uint64_t ndefault);

	// Barrier: wait until all table data is transmitted.
	void HandleFlush(const std::string& d, const RPCInfo& rpc);
	void HandleApply(const std::string& d, const RPCInfo& rpc);

	// Enable or disable triggers
	void HandleEnableTrigger(const std::string& d, const RPCInfo& rpc);

	// terminate iteration
	void HandleTermNotification(const std::string& d, const RPCInfo& rpc);

	void HandleRunKernel(const std::string& d, const RPCInfo& rpc);
	void HandleShutdown(const std::string& d, const RPCInfo& rpc);

	void HandleWorkerList(const std::string& d, const RPCInfo& rpc);

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

	void merge_net_stats();
	Stats& get_stats(){
		return stats_;
	}

private:
	void registerHandlers();
	void registerWorker();

//	void waitKernel();
	void runKernel();
	void finishKernel();

	void sendReply(const RPCInfo& rpc, const bool res=true);

	void HandlePutRequestReal(const KVPairData& put);

//functions for checkpoint
	void initialCP(CheckpointType cpType);
	void HandleStartCheckpoint(const std::string& d, const RPCInfo& rpc);
	void HandleFinishCheckpoint(const std::string& d, const RPCInfo& rpc);
	void HandleRestore(const std::string& d, const RPCInfo& rpc);
	void HandleCheckpointSig(const std::string& d, const RPCInfo& rpc);

	bool startCheckpoint(const int epoch);
	bool processCPSig(const int wid, const int epoch);
	bool finishCheckpoint(const int epoch);
	void restore(int epoch);

	void _CP_start();
	void _sendCPFlushSig();
	void _CP_report();
	void _CP_stop();

	void _startCP_Sync();
	void _finishCP_Sync();
	SyncUnit su_cp_sig;
	void _startCP_SyncSig();
	void _finishCP_SyncSig();
	void _processCPSig_SyncSig(const int wid);
	void _startCP_Async();
	void _finishCP_Async();
	void _processCPSig_Async(const int wid);
	void _HandlePutRequest_AsynCP(const std::string& d, const RPCInfo& info);
	//assume there is only one table. General form: xx[# of table][# of worker]
	std::vector<bool> _cp_async_sig_rec;

	void removeCheckpoint(const int epoch);
//end functions for checkpoint

	typedef void (Worker::*callback_t)(const string&, const RPCInfo&);
	void RegDSPImmediate(const int type, callback_t fp);
	void RegDSPProcess(const int type, callback_t fp);
	void RegDSPDefault(callback_t fp);

	void clearUnprocessedPut();
	void _enableProcess();
	void _disableProcess();

	mutable std::recursive_mutex state_lock_;

	// The current epoch this worker is running within.
	int epoch_;

	Timer tmr_;
	bool running_;	//whether this worker is running
	bool running_kernel_;	//whether this kernel is running
	KernelRequest kreq;	//the kernel running row
	std::thread* th_ker_;

	//the following state variables are used to control too frequent signal
	bool st_will_process_;	//set at signalToProcess(), reset at HandleProcessUpdates() & HandlePnS()
	bool st_will_send_;	//set at signalToSend(), reset at HandleSendUpdates() & HandlePnS()
	bool st_will_termcheck_;	//set at signalToTermCheck(), reset at HandleTermCheck()

//	CheckpointType active_checkpoint_;
	bool st_checkpointing_;
//	typedef unordered_map<int, bool> CheckpointMap;
//	CheckpointMap checkpoint_tables_;
	Timer tmr_cp_block_;
	std::thread* th_cp_;	//for name reusing (std::thread can not be assigned)

	ConfigData config_;

	// The status of other workers.
	struct Stub{
	//	int id;
		int net_id;
		int epoch;

	//	Stub(int id) : id(id), net_id(0), epoch(0){}
		Stub() : net_id(-1), epoch(0){}
	};
	std::vector<Stub> peers_;
	std::unordered_map<int,int> nid2wid;
	SyncUnit su_regw;

	NetworkThread *network_;
	std::unordered_set<GlobalTableBase*> dirty_tables_;

	Stats stats_;

	MsgDriver driver;
	bool pause_pop_msg_;
	ReplyHandler rph;

	// XXX: evolving graph
	SyncUnit su_neigh;
};


}

#endif /* WORKER_H_ */
