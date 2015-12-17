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
#include <chrono>

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");
DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);

namespace dsm {

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

	// HACKHACKHACK - register ourselves with any existing tables
	TableRegistry::Map &t = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i){
		i->second->set_helper(this);
	}

	registerHandlers();
}

int Worker::peer_for_shard(int table, int shard) const{
	return TableRegistry::Get()->tables()[table]->owner(shard);
}

void Worker::Run(){
//	KernelLoop();
	KernelLoop2();
}

Worker::~Worker(){
	running_ = false;

	for(int i = 0; i < peers_.size(); ++i){
		delete peers_[i];
	}
}

void Worker::KernelLoop(){
	VLOG(1) << "Worker " << config_.worker_id() << " registering...";
	RegisterWorkerRequest req;
	req.set_id(id());
	network_->Send(0, MTYPE_REGISTER_WORKER, req);

	KernelRequest kreq;

	while(running_){
		Timer idle;

		while(!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)){
			CheckNetwork();
			this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));

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

		KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
		KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
		DSMKernel* d = kernels_[id];

		if(!d){
			d = helper->create();
			kernels_[id] = d;
			d->initialize_internal(this, kreq.table(), kreq.shard());
			d->InitKernel();
		}

		MarshalledMap args;
		args.FromMessage(kreq.args());
		d->set_args(args);

		if(this->id() == 1 && FLAGS_sleep_hack > 0){
			this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_hack));
		}

		// Run the user kernel
		helper->Run(d, kreq.method());

		KernelDone kd;
		kd.mutable_kernel()->CopyFrom(kreq);
		TableRegistry::Map &tmap = TableRegistry::Get()->tables();
		for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
			GlobalTableBase* t = i->second;
			VLOG(1)<<"Kernel Done";
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
		network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

		VLOG(1) << "Kernel finished: " << kreq;
		DumpProfile();
	}
}

void Worker::KernelLoop2(){
	VLOG(1) << "Worker " << config_.worker_id() << " registering...";
	RegisterWorkerRequest req;
	req.set_id(id());
	network_->Send(0, MTYPE_REGISTER_WORKER, req);

	KernelRequest kreq;

	while(running_){
		Timer idle;

		while(!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)){
			CheckNetwork();
			this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));

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

		KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
		KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
		DSMKernel* d = kernels_[id];

		if(!d){
			d = helper->create();
			kernels_[id] = d;
			d->initialize_internal(this, kreq.table(), kreq.shard());
			d->InitKernel();
		}

		MarshalledMap args;
		args.FromMessage(kreq.args());
		d->set_args(args);

		if(this->id() == 1 && FLAGS_sleep_hack > 0){
			this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_hack));
		}

		// Run the user kernel
		helper->Run(d, kreq.method());

		KernelDone kd;
		kd.mutable_kernel()->CopyFrom(kreq);
		TableRegistry::Map &tmap = TableRegistry::Get()->tables();
		for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
			GlobalTableBase* t = i->second;
			VLOG(1)<<"Kernel Done";
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
		network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

		VLOG(1) << "Kernel finished: " << kreq;
		DumpProfile();
	}
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
	if(!running_){
		//clear received buffer without processing its content
		VLOG(1) << "Clearing data receiving buffer after worker ends.";
		while(network_->TryRead(Task::ANY_SRC, MTYPE_PUT_REQUEST));
		return;
	}
	lock_guard<recursive_mutex> sl(state_lock_);

	KVPairData put;
	while(network_->TryRead(Task::ANY_SRC, MTYPE_PUT_REQUEST, &put)){
		if(put.marker() != -1){
			UpdateEpoch(put.source(), put.marker());
			continue;
		}

		VLOG(2) << "Read put request of size: " << put.kv_data_size() << " for "
							<< MP(put.table(), put.shard());

		MutableGlobalTableBase *t = TableRegistry::Get()->mutable_table(put.table());
		t->ApplyUpdates(put);

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

void Worker::HandleSwapRequest(const SwapTable& req, EmptyMessage *resp, const RPCInfo& rpc){
	MutableGlobalTableBase *ta = TableRegistry::Get()->mutable_table(req.table_a());
	MutableGlobalTableBase *tb = TableRegistry::Get()->mutable_table(req.table_b());

	ta->local_swap(tb);
}

void Worker::HandleClearRequest(const ClearTable& req, EmptyMessage *resp, const RPCInfo& rpc){
	MutableGlobalTableBase *ta = TableRegistry::Get()->mutable_table(req.table());

	for(int i = 0; i < ta->num_shards(); ++i){
		if(ta->is_local_shard(i)){
			ta->get_partition(i)->clear();
		}
	}
}

void Worker::HandleShardAssignment(const ShardAssignmentRequest& shard_req, EmptyMessage *resp,
		const RPCInfo& rpc){
//  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
	for(int i = 0; i < shard_req.assign_size(); ++i){
		const ShardAssignment &a = shard_req.assign(i);
		GlobalTableBase *t = TableRegistry::Get()->table(a.table());
		int old_owner = t->owner(a.shard());
		t->get_partition_info(a.shard())->sinfo.set_owner(a.new_worker());

		VLOG(3) << "Setting owner: " << MP(a.shard(), a.new_worker());

		if(a.new_worker() == id() && old_owner != id()){
			VLOG(1) << "Setting self as owner of " << MP(a.table(), a.shard());

			// Don't consider ourselves canonical for this shard until we receive updates
			// from the old owner.
			if(old_owner != -1){
				LOG(INFO)<< "Setting " << MP(a.table(), a.shard())
				<< " as tainted.  Old owner was: " << old_owner
				<< " new owner is :  " << id();
				t->get_partition_info(a.shard())->tainted = true;
			}
		} else if (old_owner == id() && a.new_worker() != id()){
			VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
			// A new worker has taken ownership of this shard.  Flush our data out.
			t->get_partition_info(a.shard())->dirty = true;
			dirty_tables_.insert(t);
		}
	}
}

void Worker::HandleFlush(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc){
	Timer net;

	TableRegistry::Map &tmap = TableRegistry::Get()->tables();
	for(TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i){
		MutableGlobalTableBase* t = dynamic_cast<MutableGlobalTableBase*>(i->second);
		if(t){
			t->SendUpdates();
		}
	}

	network_->Flush();
	stats_["network_time"] += net.elapsed();
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

void Worker::HandleApply(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc){
	HandlePutRequest();
}

void Worker::HandleEnableTrigger(const EnableTrigger& req, EmptyMessage *resp, const RPCInfo& rpc){

	TableRegistry::Get()->tables()[req.table()]->trigger(req.trigger_id())->enable(req.enable());
}

void Worker::HandleTermNotification(const TerminationNotification& req, EmptyMessage* resp,
		const RPCInfo& rpc){
	GlobalTableBase *ta = TableRegistry::Get()->table(0);              //we have only 1 table, index 0
	DLOG(INFO)<<"worker "<<id()<<" get a termination notification.";
	for(int i = 0; i < ta->num_shards(); ++i){
		if(ta->is_local_shard(i)){
			ta->get_partition(i)->terminate();
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
	if(NetworkImplMPI::GetInstance()->id() == 0) return false;

	Worker w(conf);
	w.Run();
	Stats s = w.get_stats();
	s.Merge(NetworkThread2::Get()->stats);
	VLOG(1) << "Worker stats: \n" << s.ToString("[W"+to_string(conf.worker_id())+"]");
	return true;
}

} // end namespace
