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

namespace dsm {

static void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

Worker::Worker(const ConfigData &c){
	epoch_ = -1;
	active_checkpoint_ = CP_NONE;

	network_ = NetworkThread::Get();

	config_.CopyFrom(c);
	config_.set_worker_id(network_->id() - 1);

	num_peers_ = config_.num_workers();
	peers_.resize(num_peers_);
	for(int i = 0; i < num_peers_; ++i){
		peers_[i] = new Stub(i + 1);
	}

	running_ = true;
	running_kernel_=false;

	driver_paused_=false;

	// HACKHACKHACK - register ourselves with any existing tables
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		i->second->set_helper(this);
	}

	registerHandlers();
}
Worker::~Worker(){
	running_ = false;

	for(int i = 0; i < peers_.size(); ++i){
		delete peers_[i];
	}
}

int Worker::ownerOfShard(int table, int shard) const{
	return TableRegistry::Get()->tables()[table]->owner(shard);
}

void Worker::registerWorker(){
	VLOG(1) << "Worker " << config_.worker_id() << " registering...";
	RegisterWorkerRequest req;
	req.set_id(id());
	network_->Send(0, MTYPE_REGISTER_WORKER, req);
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
	while(running_){
		while(network_->TryReadAny(data, &info.source, &info.tag)){
			DLOG_IF(INFO,info.tag!=4)<<"get pkg from "<<info.source<<" to "<<network_->id()<<", type "<<info.tag
					<<", queue length "<<driver.queSize()<<", current paused="<<driver_paused_;
			driver.pushData(data,info);
		}
		Sleep();
		while(!driver_paused_ && !driver.empty()){
			driver.popData();
		}
	}
}

//void Worker::KernelLoop(){
//	while(running_){
//		waitKernel();
//		if(!running_){
//			return;
//		}
//
//		runKernel();
//
//		finishKernel();
//
//		DumpProfile();
//	}
//}
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

	checkpointing_=false;
//	if(id()==0)	//hack for strange synchronization problem
//		Sleep();

	// Run the user kernel
	helper->Run(d, kreq.method());

	delete d;
}
void Worker::finishKernel(){
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

void Worker::CheckNetwork(){
	Timer net;
	CheckForMasterUpdates();

	// Flush any tables we no longer own.
	for(unordered_set<GlobalTableBase*>::iterator i = dirty_tables_.begin(); i != dirty_tables_.end();
			++i){
		MutableGlobalTableBase *mg = dynamic_cast<MutableGlobalTableBase*>(*i);
		if(mg){
			mg->SendUpdates();
		}
	}

	dirty_tables_.clear();
	stats_["network_time"] += net.elapsed();
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
	LOG(INFO)<<"Not implemented. (signal the master to perform this)";
}
void Worker::realClear(const int tid){
	LOG(INFO)<<"Not implemented. (signal the master to perform this)";
}

void Worker::UpdateEpoch(int peer, int peer_epoch){
	LOG(FATAL)<<"call a strange function";

//	lock_guard<recursive_mutex> sl(state_lock_);
//	VLOG(1) << "Got peer marker: " << MP(peer, MP(epoch_, peer_epoch));
//	if(epoch_ < peer_epoch){
//		LOG(INFO)<< "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);
//
//		checkpoint_tables_.clear();
//		TableRegistry::Map &t = TableRegistry::Get()->tables();
//		for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
//			checkpoint_tables_.insert(make_pair(i->first, true));
//		}
//
////		StartCheckpoint(peer_epoch, CP_ROLLING);
//	}
//
//	peers_[peer]->epoch = peer_epoch;
//
//	bool checkpoint_done = true;
//	for(int i = 0; i < peers_.size(); ++i){
//		if(peers_[i]->epoch != epoch_){
//			checkpoint_done = false;
//			VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
//		}
//	}
//
//	if(checkpoint_done){
//		FinishCheckpoint();
//	}
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
	network_->Send(dstWorkerID + 1, MTYPE_PUT_REQUEST, put);
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

void Worker::sendReply(const RPCInfo& rpc){
	ReplyMessage rm;
	rm.set_type(static_cast<MessageTypes>(rpc.tag));
	network_->Send(rpc.source, MTYPE_REPLY, rm);
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
//	while(network_->TryRead(config_.master_id(), MTYPE_START_CHECKPOINT, &checkpoint_msg)){
//		for(int i = 0; i < checkpoint_msg.table_size(); ++i){
//			checkpoint_tables_.insert(make_pair(checkpoint_msg.table(i), true));
//		}
//
//		StartCheckpoint(checkpoint_msg.epoch(), (CheckpointType)checkpoint_msg.checkpoint_type());
//	}
//
//	while(network_->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT, &empty)){
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
	VLOG(1) << "Worker stats: \n" << s.ToString("[W"+to_string(conf.worker_id())+"]");
	return true;
}

} // end namespace
