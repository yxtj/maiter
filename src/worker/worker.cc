#include "worker.h"
#include "table/table-registry.h"
#include "util/common.h"
#include "kernel/kernel.h"
//TODO: change back after message-driven is finished
#include "net/NetworkThread2.h"
#include "net/NetworkImplMPI.h"
#include "net/Task.h"
#include <string>
#include <thread>
#include <functional>
#include <chrono>

#include "dbg/getcallstack.h"

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");
DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);

namespace dsm {

static void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

struct Worker::Stub: private noncopyable{
	int32_t id;
	int32_t epoch;

	Stub(int id) :
			id(id), epoch(0){
	}
};

Worker::Worker(const ConfigData &c){
	epoch_ = 0;
	active_checkpoint_ = CP_NONE;

	network_ = NetworkThread2::Get();

	config_.CopyFrom(c);
	config_.set_worker_id(network_->id() - 1);

	num_peers_ = config_.num_workers();
	peers_.resize(num_peers_);
	for(int i = 0; i < num_peers_; ++i){
		peers_[i] = new Stub(i + 1);
	}

	running_ = true;
	running_kernel_=false;

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

int Worker::peer_for_shard(int table, int shard) const{
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
			driver.pushData(data,info);
		}
		Sleep();
		while(!driver.empty()){
			driver.popData();
		}
	}
}

void Worker::KernelLoop(){
	while(running_){
		waitKernel();
		if(!running_){
			return;
		}

		runKernel();

		finishKernel();

		DumpProfile();
	}
}

// HandleRunKernel2() and HandleShutdown2() can end this waiting
void Worker::waitKernel(){
	Timer idle;
//	while(!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)){
	while(!running_kernel_){
//		CheckNetwork();
		Sleep();
		if(!running_){
			return;
		}
	}
	stats_["idle_time"] += idle.elapsed();
	VLOG(1) << "Received run request for " << kreq;
	if(peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()){
		LOG(FATAL)<< "Received a shard I can't work on! : " << kreq.shard()
				<< " : " << peer_for_shard(kreq.table(), kreq.shard());
	}
}
void Worker::runKernel(){
	KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
	DSMKernel* d = helper->create();
	d->initialize_internal(this, kreq.table(), kreq.shard());
	d->InitKernel();

	MarshalledMap args;
	args.FromMessage(kreq.args());
	d->set_args(args);

	if(this->id() == 1 && FLAGS_sleep_hack > 0){
		this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_hack));
	}

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
		HandlePutRequest();
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
	HandlePutRequest();

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
	return true;
}

void Worker::UpdateEpoch(int peer, int peer_epoch){
	lock_guard<recursive_mutex> sl(state_lock_);
	VLOG(1) << "Got peer marker: " << MP(peer, MP(epoch_, peer_epoch));
	if(epoch_ < peer_epoch){
		LOG(INFO)<< "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);

		checkpoint_tables_.clear();
		TableRegistry::Map &t = TableRegistry::Get()->tables();
		for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
			checkpoint_tables_.insert(make_pair(i->first, true));
		}

		StartCheckpoint(peer_epoch, CP_ROLLING);
	}

	peers_[peer]->epoch = peer_epoch;

	bool checkpoint_done = true;
	for(int i = 0; i < peers_.size(); ++i){
		if(peers_[i]->epoch != epoch_){
			checkpoint_done = false;
			VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
		}
	}

	if(checkpoint_done){
		FinishCheckpoint();
	}
}

void Worker::StartCheckpoint(int epoch, CheckpointType type){
	lock_guard<recursive_mutex> sl(state_lock_);

	if(epoch_ >= epoch){
		LOG(INFO)<< "Skipping checkpoint; " << MP(epoch_, epoch);
		return;
	}

	epoch_ = epoch;

	File::Mkdirs(StringPrintf("%s/epoch_%05d/", FLAGS_checkpoint_write_dir.c_str(), epoch_));

	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		if(checkpoint_tables_.find(i->first) != checkpoint_tables_.end()){
			VLOG(1) << "Starting checkpoint... " << MP(id(), epoch_, epoch) << " : " << i->first;
			Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
			CHECK(t != NULL) << "Tried to checkpoint a read-only table?";

			t->start_checkpoint(
					StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
							FLAGS_checkpoint_write_dir.c_str(), epoch_, i->first));
		}
	}

	active_checkpoint_ = type;

	// For rolling checkpoints, send out a marker to other workers indicating
	// that we have switched epochs.
	if(type == CP_ROLLING){
		TableData epoch_marker;
		epoch_marker.set_source(id());
		epoch_marker.set_table(-1);
		epoch_marker.set_shard(-1);
		epoch_marker.set_done(true);
		epoch_marker.set_marker(epoch_);
		for(int i = 0; i < peers_.size(); ++i){
			network_->Send(i + 1, MTYPE_PUT_REQUEST, epoch_marker);
		}
	}

	VLOG(1) << "Starting delta logging... " << MP(id(), epoch_, epoch);
}

void Worker::FinishCheckpoint(){
	VLOG(1) << "Worker " << id() << " flushing checkpoint.";
	lock_guard<recursive_mutex> sl(state_lock_);

	active_checkpoint_ = CP_NONE;
	TableRegistry::Map &t = TableRegistry::Get()->tables();

	for(int i = 0; i < peers_.size(); ++i){
		peers_[i]->epoch = epoch_;
	}

	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->finish_checkpoint();
		}
	}

	EmptyMessage req;
	network_->Send(config_.master_id(), MTYPE_CHECKPOINT_DONE, req);
}

void Worker::SendTermcheck(int snapshot, long updates, double current){
	lock_guard<recursive_mutex> sl(state_lock_);

	TermcheckDelta req;
	req.set_wid(id());
	req.set_index(snapshot);
	req.set_delta(current);
	req.set_updates(updates);
	network_->Send(config_.master_id(), MTYPE_TERMCHECK_DONE, req);

	VLOG(1) << "termination condition of subpass " << snapshot << " worker " << network_->id()
						<< " sent to master... with total current "
						<< StringPrintf("%.05f", current);
}

void Worker::Restore(int epoch){
	lock_guard<recursive_mutex> sl(state_lock_);
	LOG(INFO)<< "Worker restoring state from epoch: " << epoch;
	epoch_ = epoch;

	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		Checkpointable* t = dynamic_cast<Checkpointable*>(i->second);
		if(t){
			t->restore(
					StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
							FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
		}
	}

	EmptyMessage req;
	network_->Send(config_.master_id(), MTYPE_RESTORE_DONE, req);
}

void Worker::SendPutRequest(int dstWorkerID, const KVPairData& put){
	network_->Send(dstWorkerID + 1, MTYPE_PUT_REQUEST, put);
}

void Worker::HandlePutRequest(){
	return;
	if(!running_ || !running_kernel_){
		//clear received buffer without processing its content
		VLOG(1) << "Clearing data receiving buffer after work finished.";
//		if(kreq.kernel()=="MaiterKernel2")
//			VLOG(1)<<"\n"<<getcallstack();
//		while(network_->TryRead(Task::ANY_SRC, MTYPE_PUT_REQUEST));
		return;
	}
	lock_guard<recursive_mutex> sl(state_lock_);

	KVPairData put;
//	while(network_->TryRead(Task::ANY_SRC, MTYPE_PUT_REQUEST, &put)){
	while(false){
		if(put.marker() != -1){
			UpdateEpoch(put.source(), put.marker());
			continue;
		}

		VLOG(2) << "Read put request of size: " << put.kv_data_size() << " for "
							<< MP(put.table(), put.shard());

		MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(put.table());
		t->MergeUpdates(put);

		// Record messages from our peer channel up until they checkpointed.
		if(active_checkpoint_ == CP_MASTER_CONTROLLED
				|| (active_checkpoint_ == CP_ROLLING && put.epoch() < epoch_)){
			if(checkpoint_tables_.find(t->id()) != checkpoint_tables_.end()){
				Checkpointable *ct = dynamic_cast<Checkpointable*>(t);
				ct->write_delta(put);
			}
		}

		if(put.done() && t->tainted(put.shard())){
			VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
			t->get_partition_info(put.shard())->tainted = false;
		}
	}
}

void Worker::FlushUpdates(){
	//VLOG(2) << "finish one pass";
	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(t){
			t->SendUpdates();
			t->TermCheck();
		}
	}
}

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
	Stats s = w.get_stats();
	s.Merge(NetworkThread2::Get()->stats);
	VLOG(1) << "Worker stats: \n" << s.ToString("[W"+to_string(conf.worker_id())+"]");
	return true;
}

} // end namespace
