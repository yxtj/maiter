/*
 * typed-global-table.hpp
 *
 *  Created on: Dec 9, 2015
 *      Author: tzhou
 */

#ifndef TABLE_TYPED_GLOBAL_TABLE_HPP_
#define TABLE_TYPED_GLOBAL_TABLE_HPP_

#include "global-table.h"
#include "local-table.h"
#include "statetable.h"
#include "table_iterator.h"
#include "table.h"
#include "tbl_widget/sharder.h"
#include "tbl_widget/term_checker.h"
#include "tbl_widget/IterateKernel.h"
#include "util/marshal.hpp"
#include "util/stringpiece.h"
#include "util/noncopyable.h"
#include <deque>
#include <utility>
#include <mutex>

#include <glog/logging.h>
#include <gflags/gflags.h>

DECLARE_double(bufmsg_portion);

namespace dsm{

template<class K, class V1, class V2, class V3>
class TypedGlobalTable:
		public MutableGlobalTable,
		public TypedTable<K, V1, V2, V3>,
		private noncopyable{
public:
	bool initialized(){
		return binit;
	}

	typedef TypedTableIterator<K, V1, V2, V3> Iterator;
	typedef NetDecodeIterator<K, V1> NetUpdateDecoder;
	virtual void Init(const TableDescriptor *tinfo){
		GlobalTable::Init(tinfo);
		// which is local cannot be known now
		for(int i = 0; i < partitions_.size(); ++i){
			partitions_[i] = create_deltaT(i);
		}
		binit = false;
	}

	void InitStateTable(){
		int64_t t=0;
		for(int i = 0; i < partitions_.size(); ++i){
			if(is_local_shard(i)){
				delete partitions_[i];
				partitions_[i] = create_localT(i);
				t+=partitions_[i]->capacity();
			}
		}
		bufmsg=std::max<int64_t>(bufmsg, static_cast<int64_t>(FLAGS_bufmsg_portion*t));
		binit = true;
	}

	int get_shard(const K& k);
	V1 get_localF1(const K& k);
	V2 get_localF2(const K& k);
	V3 get_localF3(const K& k);

	// Store the given key-value pair in this hash. If 'k' has affinity for a
	// remote thread, the application occurs immediately on the local host,
	// and the update is queued for transmission to the owner.
	void put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3);
	void put(K &&k, V1 &&v1, V2 &&v2, V3 &&v3);
	void updateF1(const K &k, const V1 &v);
	void updateF2(const K &k, const V2 &v);
	void updateF3(const K &k, const V3 &v);
	void accumulateF1(const K &k, const V1 &v); // 2 TypeGloobleTable :TypeTable
	void accumulateF2(const K &k, const V2 &v);
	void accumulateF3(const K &k, const V3 &v);

	// Return the value associated with 'k', possibly blocking for a remote fetch.
	ClutterRecord<K, V1, V2, V3> get(const K &k);
	V1 getF1(const K &k);
	V2 getF2(const K &k);
	V3 getF3(const K &k);
	bool contains(const K &k);
	bool remove(const K &k);

	TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM);

	TypedTable<K, V1, V2, V3>* localT(int idx){
		return dynamic_cast<TypedTable<K, V1, V2, V3>*>(partitions_[idx]);
	}

	PTypedTable<K, V1, V3>* deltaT(int idx){
		return dynamic_cast<PTypedTable<K, V1, V3>*>(partitions_[idx]);
	}

	virtual TypedTableIterator<K, V1, V2, V3>* get_typed_iterator(
			int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM){
		return dynamic_cast<TypedTableIterator<K, V1, V2, V3>*>(
				get_iterator(shard, bfilter,fetch_num));
	}

	TypedTableIterator<K, V1, V2, V3>* get_entirepass_iterator(int shard){
		return dynamic_cast<TypedTableIterator<K, V1, V2, V3>*>(
				partitions_[shard]->entirepass_iterator(this->helper()));
	}

	void MergeUpdates(const dsm::KVPairData& req){
//		Timer t;
		std::lock_guard<std::recursive_mutex> sl(mutex());

//		VLOG(3) << "applying updates, from " << req.source();

		if(!is_local_shard(req.shard())){
			LOG_EVERY_N(INFO, 1000)<< "Forwarding push request from: ("
				<< id()<<","<<req.shard() << ") to " << owner(req.shard());
		}

		// Changes to support centralized of triggers <CRM>
		ProtoKVPairCoder c(&req);
		int shard=req.shard();
		NetUpdateDecoder it;
		partitions_[shard]->deserializeFromNet(&c, &it);

		//TODO: optimize.
		//it had been guaranteed received shard is local by SendUpdates. But accumulateF1 check it each time
		for(; !it.done(); it.Next()){
			VLOG(3) << this->owner(shard) << ":" << shard << " read from remote "
								<< it.key() << ":" << it.value1();
			accumulateF1(it.key(), it.value1());
//			ProcessUpdatesSingle(shard,it.key());
		}
		pending_process_+=req.kv_data_size();
//		VLOG_EVERY_N(1,200)<<"merge data: "<<t.elapsed()<<" with "<<req.kv_data_size();

//		ProcessUpdates();
//		BufTermCheck();
	}

	void ProcessUpdates(){
		if(!allowProcess())
			return;
		std::lock_guard<std::recursive_mutex> sl(mutex());
		if(!allowProcess())
			return;
//		Timer t;
		//handle multiple shards
		for(int i=0;i<partitions_.size();++i){
			if(!is_local_shard(i))
				continue;
			//get the iterator of the local state table
			Iterator *it2 = get_typed_iterator(i, true);
			if(it2 == nullptr){
				DLOG(INFO)<<"invalid iterator at ProcessUpdates";
				return;
			}
			//should not use for(;!it->done();it->Next()), that will skip some entry
			while(!it2->done()){
				ProcessUpdatesSingle(it2->key(), it2->value1(), it2->value2(), it2->value3());
				it2->Next();
			}
			delete it2;
		}
//		DVLOG(1)<<"process data: "<<t.elapsed();
//		BufTermCheck();
	}
	//Process with user provided functions
	void ProcessUpdatesSingle(const K& k, V1& v1, V2& v2, V3& v3){
		IterateKernel<K, V1, V3>* kernel=
				static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel);
		//pre-process
		kernel->process_delta_v(k, v1, v2, v3);
		//perform v=v+delta_v
		//process delta_v before accumulate
		accumulateF2(k, v1);
		//invoke api, perform g(delta_v) and send messages to out-neighbors
		std::vector<std::pair<K,V1> > output;
		output.reserve(v3.size());
		kernel->g_func(k, v1, v2, v3, &output);
		//perform delta_v=0, reset delta_v after delta_v has been spread out
		updateF1(k, kernel->default_v());

		//send the buffered messages to remote state table
		for(auto& kvpair : output){
			//apply the output messages to remote state table
			accumulateF1(kvpair.first, kvpair.second);
		}
	}

	void ProcessUpdatesSingle(const int shard, const K& k){
		std::lock_guard<std::recursive_mutex> sl(mutex());
		DCHECK(shard==get_shard(k))<<"given shard for a key do not match local record";
		StateTable<K,V1,V2,V3>* pt=dynamic_cast<StateTable<K,V1,V2,V3>*>(partitions_[shard]);
		ClutterRecord<K,V1,V2,V3> c=pt->get(k);
		ProcessUpdatesSingle(c.k,c.v1,c.v2,c.v3);
	}

	Marshal<K> *kmarshal(){
		return ((Marshal<K>*)info_.key_marshal);
	}
	Marshal<V1> *v1marshal(){
		return ((Marshal<V1>*)info_.value1_marshal);
	}
	Marshal<V2> *v2marshal(){
		return ((Marshal<V2>*)info_.value2_marshal);
	}
	Marshal<V3> *v3marshal(){
		return ((Marshal<V3>*)info_.value3_marshal);
	}

protected:
	int shard_for_key_str(const StringPiece& k);
	virtual LocalTable* create_localT(int shard);
	virtual LocalTable* create_deltaT(int shard);
	bool binit;
};

static const int kWriteFlushCount = 1000000;

template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::get_shard(const K& k){
	DCHECK(this != NULL);
	DCHECK(this->info().sharder != NULL);

	Sharder<K> *sharder = (Sharder<K>*)(this->info().sharder);
	int shard = (*sharder)(k, this->info().num_shards);
	DCHECK_GE(shard, 0);
	DCHECK_LT(shard, this->num_shards());
	return shard;
}

template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::shard_for_key_str(const StringPiece& k){
	return get_shard(unmarshal(static_cast<Marshal<K>*>(this->info().key_marshal), k));
}

template<class K, class V1, class V2, class V3>
V1 TypedGlobalTable<K, V1, V2, V3>::get_localF1(const K& k){
	int shard = this->get_shard(k);

	CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

	return localT(shard)->getF1(k);
}

template<class K, class V1, class V2, class V3>
V2 TypedGlobalTable<K, V1, V2, V3>::get_localF2(const K& k){
	int shard = this->get_shard(k);

	CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

	return localT(shard)->getF2(k);
}

template<class K, class V1, class V2, class V3>
V3 TypedGlobalTable<K, V1, V2, V3>::get_localF3(const K& k){
	int shard = this->get_shard(k);

	CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

	return localT(shard)->getF3(k);
}

// Store the given key-value pair in this hash. If 'k' has affinity for a
// remote thread, the application occurs immediately on the local host,
// and the update is queued for transmission to the owner.
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	if(is_local_shard(shard)){
		localT(shard)->put(k, v1, v2, v3);
	}else{
		VLOG(1) << "not local put";
		++pending_send_;
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::put(K &&k, V1 &&v1, V2 &&v2, V3 &&v3){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	if(is_local_shard(shard)){
		localT(shard)->put(std::forward<K>(k), std::forward<V1>(v1), std::forward<V2>(v2), std::forward<V3>(v3));
	}else{
		VLOG(1) << "not local put";
		++pending_send_;
	}
}
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF1(const K &k, const V1 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF1(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
	}else{
		deltaT(shard)->update(k, v);

	}

//	PERIODIC(0.1, {
//		this->HandlePutRequests();
//	});
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF2(const K &k, const V2 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF2(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
	}else{
		VLOG(2) << "update F2 shard " << shard << " local? " << " : " << is_local_shard(shard)
							<< " : " << worker_id_;
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF3(const K &k, const V3 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF3(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
	}else{
		VLOG(2) << "update F3 shard " << shard << " local? " << " : " << is_local_shard(shard)
							<< " : " << worker_id_;
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &k, const V1 &v){ //3
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		//VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on local " << k;
		localT(shard)->accumulateF1(k, v);  //TypeTable
	}else{
		//VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on remote " << k;
		deltaT(shard)->accumulate(k, v);

		++pending_send_;
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF2(const K &k, const V2 &v){ // 1
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->accumulateF2(k, v); // Typetable

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
	}else{
		VLOG(2) << "accumulate F2 shard " << shard << " local? " << " : "
							<< is_local_shard(shard);
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF3(const K &k, const V3 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_mutex());
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->accumulateF3(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
	}else{
		VLOG(2) << "accumulate F3 shard " << shard << " local? " << " : "
							<< is_local_shard(shard) << " : " << worker_id_;
	}
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
ClutterRecord<K, V1, V2, V3> TypedGlobalTable<K, V1, V2, V3>::get(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	return localT(shard)->get(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V1 TypedGlobalTable<K, V1, V2, V3>::getF1(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	return localT(shard)->getF1(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V2 TypedGlobalTable<K, V1, V2, V3>::getF2(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	return localT(shard)->getF2(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V3 TypedGlobalTable<K, V1, V2, V3>::getF3(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
	return localT(shard)->getF3(k);
}

template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::contains(const K &k){
	int shard = this->get_shard(k);

	if(is_local_shard(shard)){
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
		std::lock_guard<std::recursive_mutex> sl(mutex());
#endif
		return localT(shard)->contains(k);
	}else{
		return false;
	}
}

template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::remove(const K &k){
	int shard = this->get_shard(k);

	if(is_local_shard(shard)){
		return localT(shard)->remove(k);
		return true;
	}else{
		return false;
	}
}

template<class K, class V1, class V2, class V3>
LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_localT(int shard){
	TableDescriptor linfo=info();
	linfo.shard = shard;
	VLOG(2) << "create local statetable " << shard;
	LocalTable* t = dynamic_cast<LocalTable*>(info_.partition_factory->New());
	t->Init(&linfo);
	return t;
}

template<class K, class V1, class V2, class V3>
LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_deltaT(int shard){
	TableDescriptor linfo=info();
	linfo.shard = shard;
	VLOG(2) << "create local deltatable " << shard;
	LocalTable* t = dynamic_cast<LocalTable*>(info_.deltaT_factory->New());
	t->Init(&linfo);
	return t;
}

template<class K, class V1, class V2, class V3>
TableIterator* TypedGlobalTable<K, V1, V2, V3>::get_iterator(
		int shard, bool bfilter, unsigned int fetch_num){
	CHECK_EQ(this->is_local_shard(shard), true)<<"should use local get_iterator";
	TableIterator* res;
	if(info().schedule_portion < 1){
		res=partitions_[shard]->schedule_iterator(this->helper(), bfilter);
	} else{
		res=partitions_[shard]->get_iterator(this->helper(), bfilter);
	}
	return dynamic_cast<TypedTableIterator<K, V1, V2, V3>*>(res);
}


}

#endif /* TABLE_TYPED_GLOBAL_TABLE_HPP_ */
