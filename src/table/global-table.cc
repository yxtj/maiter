#include "global-table.h"
#include "statetable.h"
#include "util/timer.h"
#include <gflags/gflags.h>

//static const int kMaxNetworkPending = 1 << 26;
//static const int kMaxNetworkChunk = 1 << 20;

DEFINE_int32(snapshot_interval, 99999999, "");
DECLARE_int32(bufmsg);
DECLARE_double(buftime);
DECLARE_bool(local_aggregate);

namespace dsm {


ProtoTableCoder::ProtoTableCoder(const TableData *in) :
		read_pos_(0), t_(const_cast<TableData*>(in)){
}

bool ProtoTableCoder::ReadEntryFromFile(string *k, string *v1, string *v2, string *v3){
	if(read_pos_ < t_->rec_data_size()){
		k->assign(t_->rec_data(read_pos_).key());
		v1->assign(t_->rec_data(read_pos_).value1());
		v2->assign(t_->rec_data(read_pos_).value2());
		v3->assign(t_->rec_data(read_pos_).value3());
		++read_pos_;
		return true;
	}

	return false;
}

void ProtoTableCoder::WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2,
		StringPiece v3){
	Record *a = t_->add_rec_data();
	a->set_key(k.data, k.len);
	a->set_value1(v1.data, v1.len);
	a->set_value2(v2.data, v2.len);
	a->set_value3(v3.data, v3.len);
}

ProtoKVPairCoder::ProtoKVPairCoder(const KVPairData *in) :
		read_pos_(0), t_(const_cast<KVPairData*>(in)){
}

bool ProtoKVPairCoder::ReadEntryFromNet(string *k, string *v){
	if(read_pos_ < t_->kv_data_size()){
		k->assign(t_->kv_data(read_pos_).key());
		v->assign(t_->kv_data(read_pos_).value());
		++read_pos_;
		return true;
	}

	return false;
}

void ProtoKVPairCoder::WriteEntryToNet(StringPiece k, StringPiece v){
	Arg *a = t_->add_kv_data();
	a->set_key(k.data, k.len);
	a->set_value(v.data, v.len);
}

void GlobalTable::UpdatePartitions(const ShardInfo& info){
	partinfo_[info.shard()].sinfo.CopyFrom(info);
}

GlobalTable::~GlobalTable(){
	for(int i = 0; i < partitions_.size(); ++i){
		delete partitions_[i];
	}
}

TableIterator* GlobalTable::get_iterator(int shard, bool bfilter, unsigned int fetch_num){
	return partitions_[shard]->get_iterator(this->helper(), bfilter);
}

bool GlobalTable::is_local_shard(int shard){
	if(!helper()) return false;
	return owner(shard) == helper_id();
}

bool GlobalTable::is_local_key(const StringPiece &k){
	return is_local_shard(shard_for_key_str(k));
}

void GlobalTable::Init(const TableDescriptor *info){
	Table::Init(info);
	partitions_.resize(info->num_shards);
	partinfo_.resize(info->num_shards);
}

int64_t GlobalTable::shard_size(int shard){
	if(is_local_shard(shard)){
		return partitions_[shard]->size();
	}else{
		return partinfo_[shard].sinfo.entries();
	}
}

void MutableGlobalTable::resize(int64_t new_size){
	for(int i = 0; i < partitions_.size(); ++i){
		if(is_local_shard(i)){
			partitions_[i]->resize(new_size / partitions_.size() + 1);
		}
	}
}

void MutableGlobalTable::swap(GlobalTableBase *b){
	helper()->realSwap(this->id(), b->id());
//	SwapTable req;
//
//	req.set_table_a(this->id());
//	req.set_table_b(b->id());
//	VLOG(2) << StringPrintf("Sending swap request (%d <--> %d)", req.table_a(), req.table_b());
//
//	helper()->SyncSwapRequest(req);
}

void MutableGlobalTable::clear(){
	helper()->realClear(this->id());
//	ClearTable req;
//
//	req.set_table(this->id());
//	VLOG(2) << StringPrintf("Sending clear request (%d)", req.table());
//
//	helper()->SyncClearRequest(req);
}

void MutableGlobalTable::start_checkpoint(const string& pre){
	for(int i = 0; i < partitions_.size(); ++i){
		if(is_local_shard(i)){
			LocalTable *t = partitions_[i];
			t->start_checkpoint(pre + helper()->genCPNameFilePart(id(),i));
		}
	}
}

void MutableGlobalTable::write_message(const KVPairData& d){
	if(!is_local_shard(d.shard())){
		LOG(INFO) << "Ignoring delta write for forwarded data";
		return;
	}

	partitions_[d.shard()]->write_message(d);
}

void MutableGlobalTable::finish_checkpoint(){
	for(int i = 0; i < partitions_.size(); ++i){
		LocalTable *t = partitions_[i];

		if(is_local_shard(i)){
			t->finish_checkpoint();
		}
	}
}

void MutableGlobalTable::restore(const string& pre){
	for(int i = 0; i < partitions_.size(); ++i){
		LocalTable *t = partitions_[i];
		if(is_local_shard(i)){
			t->restore(pre + helper()->genCPNameFilePart(id(),i));
		}else{
			t->clear();
		}
	}
}

//void MutableGlobalTable::HandlePutRequests(){
//	if(helper()){
//		helper()->HandlePutRequest();
//	}
//}

void MutableGlobalTable::resetProcessMarker(){
	pending_process_ = 0;
	tmr_process.Reset();
}

bool MutableGlobalTable::canProcess(){
	return pending_process_ > bufmsg
			|| (pending_process_ !=0 && tmr_process.elapsed() > FLAGS_buftime);
}

void MutableGlobalTable::BufProcessUpdates(){
	if(canProcess()){
		VLOG(3) << "accumulate pending process " << pending_process_ << ", in "<<tmr_process.elapsed();
		ProcessUpdates();
		resetProcessMarker();
	}
}

void MutableGlobalTable::resetSendMarker(){
	pending_send_ = 0;
	tmr_send.Reset();
}

bool MutableGlobalTable::canSend(){
	return pending_send_ > bufmsg
			|| (pending_send_ != 0 && tmr_send.elapsed() > FLAGS_buftime);
}

void MutableGlobalTable::BufSendUpdates(){
	if(canSend()){
		VLOG(3) << "accumulate pending send " << pending_send_ << ", in "<<tmr_send.elapsed();
		SendUpdates();
		resetSendMarker();
	}
}

void MutableGlobalTable::SendUpdates(){
	// prepare
	if(FLAGS_local_aggregate){
		setUpdatesFromAggregated();
	}
	// send
	int n = num_shards();
	for(int i = 0; i < n; ++i){
		if(is_local_shard(i))
			continue;
		KVPairData& put=update_buffer[i];
		helper()->realSendUpdates(owner(i), put);
		put.clear_kv_data();
	}
	// reset
	pending_send_ = 0;
}

void MutableGlobalTable::setUpdatesFromAggregated(){	// aggregated way
	for(int i = 0; i < partitions_.size(); ++i){
		LocalTable *t = partitions_[i];
		if(!is_local_shard(i) && (get_partition_info(i)->dirty || !t->empty())){
			KVPairData& put=update_buffer[i];

			ProtoKVPairCoder c(&put);
			t->serializeToNet(&c);
			t->clear();
			VLOG(3) << "Done with update for (" << t->id() << "," << t->shard() << ")";
		}
	}
}

void MutableGlobalTable::addIntoUpdateBuffer(int shard, Arg& arg){	// non-aggregated way
	KVPairData& put=update_buffer[shard];
	Arg* p = put.add_kv_data();
	*p=std::move(arg);
}

bool MutableGlobalTable::canPnS(){
	auto m = pending_process_ >= pending_send_ ? pending_process_ : pending_send_;
	return// m > FLAGS_bufmsg
			//||
			(m != 0 && tmr_send.elapsed() > FLAGS_buftime);
}

bool MutableGlobalTable::canTermCheck(){
	PERIODIC(FLAGS_snapshot_interval, {
		return true;
	});
	return false;
}

void MutableGlobalTable::BufTermCheck(){
	if(canTermCheck()){
		this->TermCheck();
	}
}

void MutableGlobalTable::TermCheck(){
	double total_current = 0;
	long total_updates = 0;
	for(int i = 0; i < partitions_.size(); ++i){
		if(is_local_shard(i)){
			LocalTable *t = partitions_[i];
			double partF2;
			long partUpdates;
			t->termcheck(StringPrintf("snapshot/iter%d-part%d", snapshot_index, i),
					&partUpdates, &partF2);
			total_current += partF2;
			total_updates += partUpdates;
		}
	}
	if(helper()){
		helper()->realSendTermCheck(snapshot_index, total_updates, total_current);
	}

	snapshot_index++;
}

int MutableGlobalTable::pending_write_bytes(){
	int64_t s = 0;
	for(int i = 0; i < partitions_.size(); ++i){
		LocalTable *t = partitions_[i];
		if(!is_local_shard(i)){
			s += t->size();
		}
	}
	return s;
}

void MutableGlobalTable::local_swap(GlobalTableBase *b){
	CHECK(this != b);

	MutableGlobalTable *mb = dynamic_cast<MutableGlobalTable*>(b);
	std::swap(partinfo_, mb->partinfo_);
	std::swap(partitions_, mb->partitions_);
	std::swap(cache_, mb->cache_);
	std::swap(pending_send_, mb->pending_send_);
}

void MutableGlobalTable::InitUpdateBuffer(){
	int n=num_shards();
	VLOG(0)<<"num-shards: "<<n;
	update_buffer.resize(n);
	for(int i = 0; i < n; ++i){
		if(is_local_shard(i))
			continue;
		KVPairData& put=update_buffer[i];
		put.set_shard(i);
		put.set_source(helper()->id());
		put.set_table(id());
		put.set_epoch(helper()->epoch());
		// the field kv_data is filled and cleared in the sending function
		put.set_done(true);
	}
}

} // namespace dsm
