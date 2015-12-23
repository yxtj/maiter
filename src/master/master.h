#ifndef MASTER_H_
#define MASTER_H_

#include "util/common.h"
#include "msg/message.pb.h"
#include "kernel/kernel.h"
#include "table/TableHelper.h"
#include "table/table-registry.h"
#include "net/NetworkThread2.h"
#include "net/RPCInfo.h"

#include "run-descriptor.h"
#include "driver/MsgDriver.h"
#include "driver/tools/ReplyHandler.h"
#include "driver/tools/SyncUnit.h"

#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace dsm {

class WorkerState;
class TaskState;
//class NetworkThread2;

class Master: public TableHelper{
public:
	Master(const ConfigData &conf);
	~Master();

	//TableHelper methods
	int id() const{
		return -1;
	}
	int epoch() const{
		return kernel_epoch_;
	}
	int peer_for_shard(int table, int shard) const{
		return tables_[table]->owner(shard);
	}
	void SendPutRequest(int dstWorkerID, const KVPairData& put){
	}
	void HandlePutRequest(){}
	void FlushUpdates(){}
	void SendTermcheck(int index, long updates, double current){}

	void SyncSwapRequest(const SwapTable& req);
	void SyncClearRequest(const ClearTable& req);

	void run_all(RunDescriptor r);
	void run_one(RunDescriptor r);
	void run_range(RunDescriptor r, const std::vector<int>& shards);

	// N.B.  All run_* methods are blocking.
	void run_all(const string& kernel, const string& method, GlobalTableBase* locality){
		run_all(RunDescriptor(kernel, method, locality));
	}

	//maiter program
	template<class K, class V, class D>
	void run_maiter(MaiterKernel<K, V, D>* maiter){
		if(maiter->sharder == NULL){
			LOG(FATAL)<<"sharder is not specified in current kernel";
			return;
		}

		run_all("MaiterKernel1", "run", maiter->table);

		if(maiter->iterkernel != NULL){
			run_all("MaiterKernel2", "map", maiter->table);
		}

		if(maiter->termchecker != NULL){
			run_all("MaiterKernel3", "run", maiter->table);
		}
	}

	// Run the given kernel function on one (arbitrary) worker node.
	void run_one(const string& kernel, const string& method, GlobalTableBase* locality){
		run_one(RunDescriptor(kernel, method, locality));
	}

	// Run the kernel function on the given set of shards.
	void run_range(const string& kernel, const string& method,
			GlobalTableBase* locality, const std::vector<int>& shards){
		run_range(RunDescriptor(kernel, method, locality), shards);
	}

	void enable_trigger(const TriggerID triggerid, int table, bool enable);

	void run(RunDescriptor r);

	template<class T>
	T& get_cp_var(const string& key, T defval = T()){
		if(!cp_vars_.contains(key)){
			cp_vars_.put(key, defval);
		}
		return cp_vars_.get<T>(key);
	}

	void barrier();
	void barrier2();

	// Blocking.  Instruct workers to save table and kernel state.
	// When this call returns, all requested tables in the system will have been
	// committed to disk.
	void checkpoint();

	//Non-Blocking, termination check
	bool termcheck();
	void termcheck2();

	// Attempt restore from a previous checkpoint for this job.  If none exists,
	// the process is left in the original state, and this function returns false.
	bool restore();

private:
	std::thread tmsg;
	void MsgLoop();
	void registerHandlers();
	void handleReply(const std::string&, const RPCInfo&);
	void addReplyHandler(const int mtype, void (Master::*fp)(),const bool spwanThread=false);

	//helpers for registering message handlers
	typedef void (Master::*callback_t)(const string&, const RPCInfo&);
	void RegDSPImmediate(const int type, callback_t fp, bool spawnThread=false);
	void RegDSPProcess(const int type, callback_t fp, bool spawnThread=false);
	void RegDSPDefault(callback_t fp);

	SyncUnit su_swap;
	SyncUnit su_clear;

	void handleRegisterWorker(const std::string& d, const RPCInfo& info);
	SyncUnit su_regw;

	SyncUnit su_wflush;
	SyncUnit su_wapply;

	void handleKernelDone(const std::string& d, const RPCInfo& info);
	SyncUnit su_kerdone;

	void finishKernel();

	void shutdownWorkers();

	std::condition_variable cv_cp; //for wait_for(), only be notified after termination
//	std::std::vector<SyncUnit> su_cpdone;
//	void handleCheckpointDone(const std::string& d, const RPCInfo& info);
	void start_checkpoint();
	void start_worker_checkpoint(int worker_id, const RunDescriptor& r);
	void finish_worker_checkpoint(int worker_id, const RunDescriptor& r);
	void finish_checkpoint();
	SyncUnit su_restore;

	void handleTermcheckDone(const std::string& d, const RPCInfo& info);
	SyncUnit su_term;
	void terminate_iteration();

	WorkerState* worker_for_shard(int table, int shard);

	// Find a worker to run a kernel on the given table and shard.  If a worker
	// already serves the given shard, return it.  Otherwise, find an eligible
	// worker and assign it to them.
	WorkerState* assign_worker(int table, int shard);

	void send_table_assignments();
	SyncUnit su_tassign;

	bool steal_work(const RunDescriptor& r, int idle_worker, double avg_time);
	void assign_tables();
	void assign_tasks(const RunDescriptor& r, std::vector<int> shards);
	int dispatch_work(const RunDescriptor& r);

	void dump_stats();
	int reap_one_task();

	ConfigData config_;
	int checkpoint_epoch_;
	int termcheck_epoch_;
	int kernel_epoch_;

	MarshalledMap cp_vars_;

	RunDescriptor current_run_;
	double current_run_start_;
	int dispatched_; //# of dispatched tasks
	int finished_; //# of finished tasks

	bool shards_assigned_;

	bool checkpointing_;
	bool running_;
	bool kernel_terminated_;

	// Used for interval checkpointing.
	double last_checkpoint_;
	double last_termcheck_;
	Timer* barrier_timer;
	ofstream conv_track_log;
	ofstream sync_track_log;
	int iter;

	std::vector<WorkerState*> workers_;
	std::unordered_map<int, WorkerState*> netId2worker_;//map network id (rpc source) to worker

	typedef map<string, MethodStats> MethodStatsMap;
	MethodStatsMap method_stats_;

	TableRegistry::Map& tables_;
	NetworkThread2* network_;
	MsgDriver driver_;
	ReplyHandler rph_;

	Timer runtime_;
};

}

#endif /* MASTER_H_ */
