#include "worker.h"
#include "table/table-registry.h"
#include "util/common.h"
#include "kernel/kernel.h"
#include "kernel/DSMKernel.h"
#include "net/NetworkThread.h"
#include "net/Task.h"
#include <string>
#include <thread>
#include <functional>
#include <chrono>
#include <fstream>

#include "dbg/getcallstack.h"

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");

namespace dsm {

static void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

Worker::Worker(const ConfigData &c){
	epoch_ = -1;

	network_ = NetworkThread::Get();

	config_.CopyFrom(c);

	peers_.resize(config_.num_workers());
	peers_[id()].net_id=network_->id();
	nid2wid.reserve(config_.num_workers()+1);

	running_ = true;
	running_kernel_=false;

	st_will_process_=false;
	st_will_send_=false;
	st_will_termcheck_=false;

	pause_pop_msg_=false;

	th_ker_=nullptr;
	th_cp_=nullptr;

	// HACKHACKHACK - register ourselves with any existing tables
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		i->second->set_helper(this);
	}

	registerHandlers();
}
Worker::~Worker(){
	running_ = false;
	if(th_ker_ && th_ker_->joinable())
		th_ker_->join();
	delete th_ker_;
	delete th_cp_;
}

int Worker::ownerOfShard(int table, int shard) const{
	return TableRegistry::Get()->tables()[table]->owner(shard);
}

void Worker::registerWorker(){
	VLOG(1) << "Worker " << id() << " registering...";
	RegisterWorkerRequest req;
	req.set_id(id());
	//TODO: check register failure. if no reply within x seconds, register again.
	network_->Send(config_.master_id(), MTYPE_WORKER_REGISTER, req);
}

void Worker::Run(){
	registerWorker();
//	thread t(bind(&Worker::MsgLoop,this));
	MsgLoop();
//	thread t(bind(&Worker::KernelLoop,this));
//	KernelLoop();
//	t.join();
}

void Worker::MsgLoop(){
	string data;
	RPCInfo info;
	info.dest=network_->id();
	unsigned cnt_idle_loop=0;
	static constexpr unsigned SLEEP_CNT=256;
//	double t1=0,t2=0;
	while(running_){
		bool idle=true;
//		Timer tmr;
		int cnt=200;
		while(--cnt>=0 && network_->TryReadAny(data, &info.source, &info.tag)){
//			DLOG_IF(INFO,true || info.tag!=4 || driver.queSize()%1000==100)<<"get pkg from "<<info.source<<" to "<<network_->id()
//					<<", type "<<info.tag<<", queue length "<<driver.queSize()<<", current paused="<<pause_pop_msg_;
			driver.pushData(data,info);
			idle=false;
		}
//		t1=tmr.elapsed();
//		static int count=0;
//		static double time=0;
//		PERIODIC(2, {
//			DLOG(INFO)<<"receiving time: "<<t1<<"\tprocessing time: "<<t2
//				<<"\tavg proc. time: "<<(count==0?0:t2/count)<<"\t# of pkt: "<<count
//				<<"\nStates: process: "<<st_will_process_<<"\tsend: "<<st_will_send_<<"\tcheckpoint: "<<st_checkpointing_
//				<<"\ndriver queue: "<<driver.queSize()
//				<<"\tunpicked_pkgs queue: "<<network_->unpicked_pkgs()
//				<<"\tpending_pkgs queue: "<<network_->pending_pkgs();
//			t2=0;
//			count=0;
//			time=0;
//		});
//		DLOG_IF_EVERY_N(INFO,driver.queSize()>1000,10)<<"driver queue length: "<<driver.queSize();
//		DLOG_IF_EVERY_N(INFO,network_->unpicked_pkgs()>1000,10)<<"unpicked_pkgs queue length: "<<network_->unpicked_pkgs();
//		DLOG_IF_EVERY_N(INFO,network_->pending_pkgs()>1000,10)<<"pending_pkgs queue length: "<<network_->pending_pkgs();
//		tmr.Reset();
		while(!pause_pop_msg_ && !driver.empty()){
//			int v=driver.back().second.tag;

			driver.popData();
			idle=false;

//			PERIODIC(2,{
//				VLOG(1)<<"process time: "<<t.elapsed()<<" for type "<<v;
//			});
//			DLOG_IF_EVERY_N(INFO,v==MTYPE_PUT_REQUEST,200)<<"merge data: "<<t.elapsed();
//			++count;
		}
//		t2=tmr.elapsed();
		if(idle && cnt_idle_loop++%SLEEP_CNT==0)
			Sleep();
	}
}

void Worker::KernelProcess(){
	stats_["idle_time"]+=tmr_.elapsed();

	DVLOG(1)<<"Start kernel process";

	runKernel();
	finishKernel();

//	DumpProfile();
	DVLOG(1)<<"Finish kernel process";

	tmr_.reset();
}

void Worker::runKernel(){
	VLOG(1)<<"Kernel started: "<<kreq;

	KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
	DSMKernel* d = helper->create();
	d->initialize_internal(this, kreq.table(), kreq.shard());
	d->InitKernel();

	MarshalledMap args;
	args.FromMessage(kreq.args());
	d->set_args(args);

	initialCP(kreq.cp_type());
	if(kreq.restore()){
		restore(kreq.restore_from_epoch());
	}

//	if(id()==0)	//hack for strange synchronization problem
//		Sleep();
	_enableProcess();

	// Run the user kernel
	helper->Run(d, kreq.method());
	if(kreq.kernel()=="MaiterKernel1")
			//&& kreq.method()=="coord")
	{
		rph.input(MTYPE_ADD_INNEIGHBOR, id());
		su_neigh.wait();
	}

	//clear the setting of checkpoint
	if(kreq.cp_type()!=CP_NONE)
		initialCP(CP_NONE);

	delete d;
}
void Worker::finishKernel(){
	//shutdown checkpoint thread if available
	if(th_cp_!=nullptr && th_cp_->joinable()){
		if(st_checkpointing_){
			VLOG(1)<<"working finished, now waiting for cp thread to terminate";
			su_cp_sig.notify();
		}
		th_cp_->join();	//this must be called before deconstructed
		delete th_cp_;
		th_cp_=nullptr;
		if(st_checkpointing_){
			su_cp_sig.reset();
			removeCheckpoint(epoch_);
		}
	}
	//TODO: stop the currently running checkpoint when the kernel finishes
	while(st_checkpointing_)
		Sleep();
	//send termination report to master
	KernelDone kd;
	kd.mutable_kernel()->CopyFrom(kreq);
	kd.set_wid(id());
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		GlobalTableBase* t = i->second;
		DVLOG(1)<<"Kernel Done of table "<<i->first;
		for(int j = 0; j < t->num_shards(); ++j){
			if(t->is_local_shard(j)){
				ShardInfo *si = kd.add_shards();
				si->set_entries(t->shard_size(j));
				si->set_owner(this->id());
				si->set_table(i->first);
				si->set_shard(j);
			}
		}
	}
	running_kernel_=false;
	network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

	VLOG(1) << "Kernel finished: " << kreq;
}

int64_t Worker::pending_kernel_bytes() const{
	int64_t t = 0;

	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase *mg = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(mg){
			t += mg->pending_write_bytes();
		}
	}

	return t;
}

bool Worker::network_idle() const{
	return network_->pending_bytes() == 0;
}

bool Worker::has_incoming_data() const{
	return !driver.empty();
}

void Worker::merge_net_stats(){
	stats_.Merge(network_->stats);
}

void Worker::realSwap(const int tid1, const int tid2){
	LOG(INFO)<<"Invalid. (signal the master to perform this)";
}
void Worker::realClear(const int tid){
	LOG(INFO)<<"Invalid. (signal the master to perform this)";
}

void Worker::realSendInNeighbor(int dstWorkerID, const InNeighborData& data){
	network_->Send(peers_[dstWorkerID].net_id , MTYPE_ADD_INNEIGHBOR, data);
}

void Worker::realSendRequest(int dstWorkerID, const ValueRequest& req) {
	network_->Send(peers_[dstWorkerID].net_id, MTYPE_VALUE_REQUEST, req);
}

void Worker::realSendTermCheck(int snapshot,
		uint64_t receives, uint64_t updates, double current, uint64_t ndefault){
	lock_guard<recursive_mutex> sl(state_lock_);

	TermcheckDelta req;
	req.set_wid(id());
	req.set_index(snapshot);
	req.set_receives(receives);
	req.set_updates(updates);
	req.set_delta(current);
	req.set_ndefault(ndefault);
	network_->Send(config_.master_id(), MTYPE_TERMCHECK_LOCAL, req);

	VLOG(1) << "termination condition of " << " worker-" << id() << " pass-" << snapshot
			<< " sent to master... with current (" << current << " , " << ndefault << ")"
			<< " progress (" << receives << " , " << updates << ")";
}

void Worker::realSendUpdates(int dstWorkerID, const KVPairData& put){
	network_->Send(peers_[dstWorkerID].net_id , MTYPE_PUT_REQUEST, put);
}

void Worker::signalToProcess(){
	if(st_will_process_)
		return;
	st_will_process_=true;
	EmptyMessage msg;
	network_->Send(network_->id(),MTYPE_LOOP_PROCESS,msg);
//	return;
//	RPCInfo info{0,0,MTYPE_LOOP_PROCESS};
//	driver.pushData(string(),info);
}
void Worker::signalToSend(){
	if(st_will_send_)
		return;
	st_will_send_=true;
	EmptyMessage msg;
	network_->Send(network_->id(),MTYPE_LOOP_SEND,msg);
//	return;
//	RPCInfo info{0,0,MTYPE_LOOP_SEND};
//	driver.pushData(string(),info);
}
void Worker::signalToTermCheck(){
	if(st_will_termcheck_)
		return;
	st_will_termcheck_=true;
	EmptyMessage msg;
	network_->Send(network_->id(),MTYPE_LOOP_TERMCHECK,msg);
//	return;
//	RPCInfo info{0,0,MTYPE_LOOP_TERMCHECK};
//	driver.pushData(string(),info);
}
void Worker::signalToPnS(){
	if(st_will_process_ && st_will_send_)
		return;
	bool bsp=st_will_process_, bss=st_will_send_;
	st_will_process_=true;
	st_will_send_=true;
	EmptyMessage msg;
	if(!bsp && !bss){
		network_->Send(network_->id(),MTYPE_LOOP_PNS,msg);
	}else if(!bsp){
		network_->Send(network_->id(),MTYPE_LOOP_PROCESS,msg);
	}else{//(!bss)
		network_->Send(network_->id(),MTYPE_LOOP_SEND,msg);
	}
//	return;
//	RPCInfo info{0,0,MTYPE_LOOP_PNS};
//	driver.pushData(string(),info);
}

void Worker::HandleRequest(const ValueRequest& req) {
	// we only have one table
	MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(0);
	t->ProcessRequest(req);
}

void Worker::HandlePutRequestReal(const KVPairData& put){
//	DVLOG(2) << "Read put request of size: " << put.kv_data_size() << " for ("
//				<< put.table()<<","<<put.shard()<<")";
		
	MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(put.table());
	t->MergeUpdates(put);

	if(put.done() && t->tainted(put.shard())){
		VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
		t->get_partition_info(put.shard())->tainted = false;
	}
}

void Worker::sendReply(const RPCInfo& rpc, const bool res){
	ReplyMessage rm;
	rm.set_type(static_cast<MessageTypes>(rpc.tag));
	rm.set_result(res);
	network_->Send(rpc.source, MTYPE_REPLY, rm);
}

void Worker::clearUnprocessedPut(){
	pause_pop_msg_=true;
	driver.abandonData(MTYPE_PUT_REQUEST);
	pause_pop_msg_=false;
}

void Worker::_enableProcess(){
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = tbl.begin(); it != tbl.end(); ++it){
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(it->second);
		if(t){
			t->enableProcess();
		}
	}
}
void Worker::_disableProcess(){
	TableRegistry::Map &tbl = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator it = tbl.begin(); it != tbl.end(); ++it){
		MutableGlobalTable *t = dynamic_cast<MutableGlobalTable*>(it->second);
		if(t){
			t->disableProcess();
		}
	}
}

bool StartWorker(const ConfigData& conf){
	if(NetworkThread::Get()->id() == conf.master_id()) return false;

	Worker w(conf);
	w.Run();
	w.merge_net_stats();
	Stats& s = w.get_stats();
	LOG(INFO) << "Worker stats: \n" << s.ToString("[W"+to_string(conf.worker_id())+"]", true);
	return true;
}

}
