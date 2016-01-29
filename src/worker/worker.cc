#include "worker.h"
#include "table/table-registry.h"
#include "util/common.h"
#include "kernel/kernel.h"
//TODO: change back after message-driven is finished
#include "net/NetworkThread.h"
#include "net/NetworkImplMPI.h"
#include "net/Task.h"
#include <string>
#include <thread>
#include <functional>
#include <chrono>

#include "dbg/getcallstack.h"

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");
DEFINE_int32(max_preread_pkg,200,"maximum number of buffered received packages before committed to application layer");

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
//	int pkt=0;
	while(running_){
//		if(network_->TryReadAny(data, &info.source, &info.tag)){
//			DLOG_IF(INFO,info.tag!=4 || driver.queSize()%1000==100)<<"get pkg from "<<info.source<<" to "<<network_->id()<<", type "<<info.tag
//					<<", queue length "<<driver.queSize()<<", current paused="<<pause_pop_msg_;
//			driver.pushData(data,info);
//		}
//		Sleep();
//		if(!pause_pop_msg_ && !driver.empty()){
//			driver.popData();
//		}
		bool idle=true;
		//Speed control. pause fetching msg when there are too many waiting for process
		if(network_->unpicked_pkgs()+network_->pending_pkgs() <= FLAGS_max_preread_pkg){
			DLOG_EVERY_N(INFO,200000)<<"Acceptable unprocessed msg. driver.len="<<driver.queSize()
					<<", unpicked="<<network_->unpicked_pkgs()<<", pending="<<network_->pending_pkgs();
			network_->doReading=true;
		}else{
			network_->doReading=false;
			DLOG_EVERY_N(INFO,20000)<<"Too many unprocessed msg. driver.len="<<driver.queSize()
					<<", unpicked="<<network_->unpicked_pkgs()<<", pending="<<network_->pending_pkgs();
		}
//		Timer tmr;
//		int cnt=200;
		while(--cnt>=0 && network_->TryReadAny(data, &info.source, &info.tag)){
			DLOG_IF(INFO,info.tag!=4 || driver.queSize()%1000==100)<<"get pkg from "<<info.source<<" to "<<network_->id()<<", type "<<info.tag
					<<", queue length "<<driver.queSize()<<", current paused="<<pause_pop_msg_;
			driver.pushData(data,info);
			idle=false;
		}
//		t1=tmr.elapsed();
//		static int count=0;
//		static double time=0;
//		if(kreq.kernel()=="MaiterKernel2"){
//		PERIODIC(1,
//				DLOG(INFO)<<"receiving time: "<<t1<<"\tprocessing time: "<<t2<<"\t avg process time: "<<t2/max(1,pkt)<<"\t real process time: "<<time/max(1,count)
//					<<"\ndriver queue length: "<<driver.queSize()
//					<<"\nunpicked_pkgs queue length: "<<network_->unpicked_pkgs()
//					<<"\npending_pkgs queue length: "<<network_->pending_pkgs();
//				);
//		}
//		DLOG_IF_EVERY_N(INFO,driver.queSize()>1000,10)<<"driver queue length: "<<driver.queSize();
//		DLOG_IF_EVERY_N(INFO,network_->unpicked_pkgs()>1000,10)<<"unpicked_pkgs queue length: "<<network_->unpicked_pkgs();
//		DLOG_IF_EVERY_N(INFO,network_->pending_pkgs()>1000,10)<<"pending_pkgs queue length: "<<network_->pending_pkgs();
//		tmr.Reset();
//		pkt=0;
		while(!pause_pop_msg_ && !driver.empty()){
//			++pkt;
//			static Timer tmr;
//			tmr.Reset();

			driver.popData();
			idle=false;

//			time+=tmr.elapsed();
//			if(++count==200){
//				VLOG(1)<<"\naverage process time (msg)="<<time/count;
//				count=0;
//				time=0;
//			}
		}
//		t2=tmr.elapsed();
		if(idle && cnt_idle_loop++%SLEEP_CNT==0)
			Sleep();
	}
}

void Worker::KernelProcess(){
	stats_["idle_time"]+=tmr_.elapsed();

	runKernel();
	finishKernel();

//	DumpProfile();
	DVLOG(1)<<"Finish kernel process";

	tmr_.Reset();
}
//
//// HandleRunKernel2() and HandleShutdown2() can end this waiting
//void Worker::waitKernel(){
//	Timer idle;
//	while(!running_kernel_){
////		CheckNetwork();
//		Sleep();
//		if(!running_){
//			return;
//		}
//	}
//	stats_["idle_time"] += idle.elapsed();
//	VLOG(1) << "Received run request for " << kreq;
//	if(ownerOfShard(kreq.table(), kreq.shard()) != config_.worker_id()){
//		LOG(FATAL)<< "Received a shard I can't work on! : " << kreq.shard()
//				<< " : " << ownerOfShard(kreq.table(), kreq.shard());
//	}
//}
void Worker::runKernel(){
	KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
	DSMKernel* d = helper->create();
	d->initialize_internal(this, kreq.table(), kreq.shard());
	d->InitKernel();

	MarshalledMap args;
	args.FromMessage(kreq.args());
	d->set_args(args);

	initialCP(kreq.cp_type());
//	if(id()==0)	//hack for strange synchronization problem
//		Sleep();
	_enableProcess();

	// Run the user kernel
	helper->Run(d, kreq.method());

	//clear the setting for checkpointing
	if(kreq.cp_type()!=CP_NONE)
		initialCP(CP_NONE);

	delete d;
}
void Worker::finishKernel(){
	//shutdown checkpoint thread if available
	if(th_cp_!=nullptr && th_cp_->joinable()){
		if(checkpointing_){
			VLOG(1)<<"working finished, now waiting for cp thread to terminate";
			su_cp_sig.notify();
		}
		th_cp_->join();	//this must be called before deconstructed
		delete th_cp_;
		th_cp_=nullptr;
		if(checkpointing_){
			su_cp_sig.reset();
			removeCheckpoint(epoch_);
		}
	}
	//TODO: cancel ongoing checkpoint when kernel finished
	while(checkpointing_)
		Sleep();
	//send termination report to master
	KernelDone kd;
	kd.mutable_kernel()->CopyFrom(kreq);
	kd.set_wid(id());
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		GlobalTableBase* t = i->second;
		VLOG(1)<<"Kernel Done of table "<<i->first;
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

//void Worker::CheckNetwork(){
//	Timer net;
//	CheckForMasterUpdates();
//
//	// Flush any tables we no longer own.
//	for(unordered_set<GlobalTableBase*>::iterator i = dirty_tables_.begin(); i != dirty_tables_.end();
//			++i){
//		MutableGlobalTableBase *mg = dynamic_cast<MutableGlobalTableBase*>(*i);
//		if(mg){
//			mg->SendUpdates();
//		}
//	}
//
//	dirty_tables_.clear();
//	stats_["network_time"] += net.elapsed();
//}

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

void Worker::SendTermcheck(int snapshot, long updates, double current){
	lock_guard<recursive_mutex> sl(state_lock_);

	TermcheckDelta req;
	req.set_wid(id());
	req.set_index(snapshot);
	req.set_delta(current);
	req.set_updates(updates);
	network_->Send(config_.master_id(), MTYPE_TERMCHECK_DONE, req);

	VLOG(1) << "termination condition of subpass " << snapshot << " worker " << id()
						<< " sent to master... with total current "
						<< StringPrintf("%.05f", current);
}

void Worker::SendPutRequest(int dstWorkerID, const KVPairData& put){
	network_->Send(peers_[dstWorkerID].net_id , MTYPE_PUT_REQUEST, put);
}

void Worker::HandlePutRequestReal(const KVPairData& put){
//	DVLOG(2) << "Read put request of size: " << put.kv_data_size() << " for ("
//				<< put.table()<<","<<put.shard()<<")";

	MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(put.table());
	t->MergeUpdates(put);
//	t->ProcessUpdates();

	if(put.done() && t->tainted(put.shard())){
		VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
		t->get_partition_info(put.shard())->tainted = false;
	}
}

//void Worker::FlushUpdates(){
//	//VLOG(2) << "finish one pass";
//	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
//	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
//		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
//		if(t){
//			t->SendUpdates();
//			t->TermCheck();
//		}
//	}
//}

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

void Worker::CheckForMasterUpdates(){
	lock_guard<recursive_mutex> sl(state_lock_);
	// Check for shutdown.
	EmptyMessage empty;

//	if(network_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &empty)){
//		VLOG(1) << "Shutting down worker " << config_.worker_id();
//		running_ = false;
//		return;
//	}

//	CheckpointRequest checkpoint_msg;
//	while(network_->TryRead(config_.master_id(), MTYPE_CHECKPOINT_START, &checkpoint_msg)){
//		for(int i = 0; i < checkpoint_msg.table_size(); ++i){
//			checkpoint_tables_.insert(make_pair(checkpoint_msg.table(i), true));
//		}
//
//		StartCheckpoint(checkpoint_msg.epoch(), (CheckpointType)checkpoint_msg.checkpoint_type());
//	}
//
//	while(network_->TryRead(config_.master_id(), MTYPE_CHECKPOINT_FINISH, &empty)){
//		FinishCheckpoint();
//	}
//
//	StartRestore restore_msg;
//	while(network_->TryRead(config_.master_id(), MTYPE_RESTORE, &restore_msg)){
//		Restore(restore_msg.epoch());
//	}
}

bool StartWorker(const ConfigData& conf){
	//TODO: change back after message-driven is finished
	if(NetworkImplMPI::GetInstance()->id() == conf.master_id()) return false;

	Worker w(conf);
	w.Run();
	w.merge_net_stats();
	Stats s = w.get_stats();
	LOG(INFO) << "Worker stats: \n" << s.ToString("[W"+to_string(conf.worker_id())+"]");
	return true;
}

} // end namespace
