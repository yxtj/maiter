/*
 * typed-global-table.hpp
 *
 *  Created on: Dec 9, 2015
 *      Author: tzhou
 *  Modified on April 26, 2017
 *      By: tzhou
 *      add support for evolving graph
 */

#ifndef TABLE_TYPED_GLOBAL_TABLE_HPP_
#define TABLE_TYPED_GLOBAL_TABLE_HPP_

#include "global-table.h"
#include "local-table.h"
#include "statetable.h"
#include "deltatable.h"
#include "msg/message.pb.h"
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
DECLARE_bool(local_aggregate);
DECLARE_int32(bufmsg);

namespace dsm{

template<class K, class V1, class V2, class V3>
class TypedGlobalTable:
		public MutableGlobalTable,
		public TypedTable<K, V1, V2, V3>,
		private noncopyable{
public:
	virtual bool initialized(){
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
		// XXX: evolving graph
		in_neighbor_cache.resize(tinfo->num_shards);
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
		InitUpdateBuffer();
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
	bool accumulateF1(const K& from, const K &to, const V1 &v);
	void accumulateF1(const K &k, const V1 &v); // 2 TypeGloobleTable :TypeTable
	void accumulateF2(const K &k, const V2 &v);
	void accumulateF3(const K &k, const V3 &v);

	// load out-neighbor and generate in-neighbor
	void add_ineighbor_from_out(const K &from, const V1 &v1, const std::vector<K>& ons);
	// fill the in-neighbor information base on all local data. "process" means whether to send processed initial delta.
	void fill_ineighbor_cache(bool process);
	void apply_inneighbor_cache_local();
	// generate and send messages of in-neighbor information
	void send_ineighbor_cache_remote();
	void clear_ineighbor_cache();
	void reset_ineighbor_bp();

	// receive in-neighbor information
	virtual void add_ineighbor(const dsm::InNeighborData& req);
	// apply graph changes
	virtual void change_graph(const K& k, const ChangeEdgeType& type, const V3& change);
	void recal_v2_from_inerghbor_cache(const K& k);

	virtual void ProcessRequest(const ValueRequest& req);

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

	virtual void MergeUpdates(const dsm::KVPairData& req){
//		Timer t;
		std::lock_guard<std::recursive_mutex> sl(get_mutex());

//		VLOG(3) << "applying updates, from " << req.source();

		// Changes to support centralized of triggers <CRM>
		int shard=req.shard();
		/*ProtoKVPairCoder c(&req);
		NetUpdateDecoder it;
		partitions_[shard]->deserializeFromNet(&c, &it);
		//TODO: optimize.
		//it had been guaranteed received shard is local by SendUpdates. But accumulateF1 check it each time
		for(; !it.done(); it.Next()){
			VLOG(3) << this->owner(shard) << ":" << shard << " read from remote "
					<< it.key() << ":" << it.value1();
			accumulateF1(it.key(), it.value1());
		}*/
		Marshal<K>* km = kmarshal();
		Marshal<V1>* vm = v1marshal();
		int size = req.kv_data_size();
		for(int i=0;i<size;++i){
			const Arg& it=req.kv_data(i);
			VLOG(3) << this->owner(shard) << ":" << shard << " read from remote "
					<< it.src() << "-" << it.key() << ":" << it.value();
			K from, to;
			V1 value1;
			km->unmarshal(it.src(), &from);

			km->unmarshal(it.key(), &to);
			vm->unmarshal(it.value(), &value1);
			accumulateF1(from, to, value1);
			//accumulateF1(to, value1);
		}
		pending_process_+=req.kv_data_size();

//		ProcessUpdates();
//		BufTermCheck();
	}

	void ProcessUpdates(){
		if(!allowProcess())
			return;
		std::lock_guard<std::recursive_mutex> sl(get_mutex());
		if(!allowProcess())
			return;
//		VLOG(1)<<"processing";
		processing_=true;
		// automatically reset processing to false when this function exits
		std::shared_ptr<bool> guard_processing(&processing_, [](bool* p){
			*p=false;
		});
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
//			int c=0;
			while(!it2->done()){
//				typedef typename StateTable<K,V1,V2,V3>::Iterator it_t;
//				it_t* p=dynamic_cast<it_t*>(it2);
//				VLOG(1)<<c++<<" p="<<p->pos<<" "<<p->parent_.size()<<" k="<<it2->key();
				ProcessUpdatesSingle(it2->key(), it2->value1(), it2->value2(), it2->value3());
				it2->Next();
//				++c;
			}
//			VLOG(1)<<c;
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
		//process
		if(kernel->is_selective()){
			// replace v with delta_v
			updateF2(k, v1);
		}else{
			//perform v=v+delta_v
			//process delta_v before accumulate
			accumulateF2(k, v1);
		}

		// get the new F2 after process
//		V2 vv2 = get_localF2(k);

		//invoke api, perform g(delta_v) and send messages to out-neighbors
		std::vector<std::pair<K,V1> > output;
		output.reserve(v3.size());
		kernel->g_func(k, v1, v2, v3, &output);

		//perform delta_v=0, reset delta_v after delta_v has been spread out
		if(!kernel->is_selective())
			updateF1(k, kernel->default_v());

		//send the buffered messages to remote state table
		handleGeneratedInformation(k, output);
	}

	void ProcessUpdatesSingle(const int shard, const K& k){
		std::lock_guard<std::recursive_mutex> sl(get_mutex());
		DCHECK(shard==get_shard(k))<<"given shard for a key do not match local record";
		StateTable<K,V1,V2,V3>* pt=dynamic_cast<StateTable<K,V1,V2,V3>*>(partitions_[shard]);
		ClutterRecord<K,V1,V2,V3> c=pt->get(k);
		ProcessUpdatesSingle(c.k,c.v1,c.v2,c.v3);
	}

	// generalized message sending function
	void handleGeneratedInformation(const K& from, std::vector<std::pair<K,V1>>& output){
		if(FLAGS_local_aggregate){
			for(auto& kvpair : output){
				// aggregate the output in local delta table and wait for generating a message later
				accumulateF1(kvpair.first, kvpair.second);
			}
		}else{
			for(auto& kvpair : output){
				// directly put the changes into a message buffer
				accumulateF1(from, kvpair.first, kvpair.second);
			}
		}
	}

	void bufferGeneratedMessage(const K& from, const K& to, const V1& delta){
		int shard = get_shard(to);
		//Arg *a = update_buffer[shard].add_kv_data();
		Arg a;
		string temp;
		kmarshal()->marshal(to, &temp);
		a.set_key(temp);
		v1marshal()->marshal(delta, &temp);
		a.set_value(temp);
		kmarshal()->marshal(from, &temp);
		a.set_src(temp);
		addIntoUpdateBuffer(shard, a);
	}

	void sendRequest(const K& local, const K& source);

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
	// XXX: evolving graph
	std::vector<std::unordered_multimap<K, std::pair<K,V1>>> in_neighbor_cache;
	std::mutex b_icache;
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

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::add_ineighbor_from_out(
		const K &from, const V1 &v1, const std::vector<K>& ons)
{
	std::lock_guard<std::recursive_mutex> lg(get_mutex());
	for(const K& to : ons){
		int shard = this->get_shard(to);
		if(is_local_shard(shard)){
			StateTable<K, V1, V2, V3> *st=dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(shard));
			st->add_ineighbor(from, to, v1);
		}else{
			in_neighbor_cache[shard].emplace(to, std::make_pair(from, v1));
		}
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::fill_ineighbor_cache(bool process){
	std::lock_guard<std::recursive_mutex> lg(get_mutex());
	IterateKernel<K, V1, V3>* kernel=
			static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel);
	V1 default_v=kernel->default_v();
	for(int i = 0; i < info().num_shards; ++i){
		if(!is_local_shard(i))
			continue;
		TypedTableIterator<K, V1, V2, V3>* it = get_entirepass_iterator(i);
		//int counter = 0;
		while(!it->done()){
			K key = it->key();
			V1 delta = it->value1();
			V2 value = it->value2();
			//counter++;
			if(process && value != default_v){
				std::vector<std::pair<K, V1> > output;
				output.reserve(it->value3().size());
				kernel->g_func(key, delta, it->value2(), it->value3(), &output);
				for(auto& p : output){
					int shard = this->get_shard(p.first);
					in_neighbor_cache[shard].emplace(p.first, std::make_pair(key, p.second));
				}
			}else{
				std::vector<K> tos = kernel->get_keys(it->value3());
				for(auto& to : tos){
					int shard = this->get_shard(to);
					in_neighbor_cache[shard].emplace(to, std::make_pair(key, delta));
				}
			}
			it->Next();
		}
		VLOG(1) << "filling in-neighbor cache on " << i;//<< " " << counter;
		delete it;
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::apply_inneighbor_cache_local(){
	std::lock_guard<std::recursive_mutex> lg(get_mutex());
	for(int i = 0; i < info().num_shards; ++i){
		if(!is_local_shard(i))
			continue;
		StateTable<K, V1, V2, V3> *st = dynamic_cast<StateTable<K,V1,V2,V3>*>(localT(i));
		for(auto& p : in_neighbor_cache[i]){
			const K& to=p.first;
			const K& from=p.second.first;
			st->add_ineighbor(from, to, p.second.second);
		}
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::send_ineighbor_cache_remote(){
	std::lock_guard<std::recursive_mutex> lg(get_mutex());
	Marshal<K> * km = kmarshal();
	Marshal<V1> * vm = v1marshal();
	int size=std::max<int>(bufmsg, 1024);
	for(size_t i=0;i<in_neighbor_cache.size();++i){
		if(is_local_shard(i))
			continue;
		auto& ref=in_neighbor_cache[i];
		int n=0;
		InNeighborData msg;
		msg.set_table(info().table_id);
		for(auto it = ref.begin(); it!=ref.end(); ++it){
			InNeighborUnit* p = msg.add_data();
			string to, from, weight;
			km->marshal(it->first,&to);
			km->marshal(it->second.first,&from);
			vm->marshal(it->second.second,&weight);
			p->set_to(to);
			p->set_from(from);
			p->set_weight(weight);
			if(++n%size == 0){
				// VLOG(1)<<"sending in-neighbor message from "<<helper_id()<<" to "<<i<<" with size "<<msg.data_size();
				info().helper->realSendInNeighbor(owner(i), msg);
				msg.clear_data();
			}
		}
		if(msg.data_size()!=0){
			// VLOG(1)<<"sending in-neighbor message from "<<helper_id()<<" to "<<i<<" with size "<<msg.data_size();
			info().helper->realSendInNeighbor(owner(i), msg);
		}
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::clear_ineighbor_cache(){
	std::lock_guard<std::recursive_mutex> lg(get_mutex());
	in_neighbor_cache.clear();
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::reset_ineighbor_bp(){
	VLOG(1)<<"reset bp";
	for(int i=0;i<partitions_.size();++i){
		if(!is_local_shard(i))
			continue;
		StateTable<K, V1, V2, V3> *st=dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(i));
		st->reset_best_pointer();
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::add_ineighbor(const dsm::InNeighborData& req){
	Marshal<K> * km = kmarshal();
	Marshal<V1> * vm = v1marshal();
	V1 default_v = static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel)->default_v();
	int size=req.data_size();
	for(int i=0;i<size;++i){
		const InNeighborUnit& p = req.data(i);
		K to, from;
		V2 value;
		km->unmarshal(p.to(), &to);
		km->unmarshal(p.from(), &from);
		vm->unmarshal(p.weight(), &value);
		int shard = this->get_shard(to);
		StateTable<K, V1, V2, V3> *st = dynamic_cast<StateTable<K,V1,V2,V3>*>(localT(shard));
		st->add_ineighbor(from, to, default_v);
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::change_graph(const K& k, const ChangeEdgeType& type, const V3& change){
	int shard = this->get_shard(k);
	if(is_local_shard(shard)){
		StateTable<K, V1, V2, V3> *st=dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(shard));
		st->change_graph(k, type, change);
	}else{
		VLOG(1) << "not local change";
		++pending_send_;
	}
}

template<class K, class V1, class V2, class V3>
inline void TypedGlobalTable<K, V1, V2, V3>::ProcessRequest(const ValueRequest & req)
{
	K key, src;
	kmarshal()->unmarshal(req.key(), &key);
	kmarshal()->unmarshal(req.source(), &src);
	int shard = this->get_shard(key);
	if(is_local_shard(shard)) {
		StateTable<K, V1, V2, V3> *st = dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(shard));
		if(st->is_best_source(key, src)) {	// the best source sends request: re-send to all other in-neighbors
			sendRequest(key, src);

			/*IterateKernel<K, V1, V3>* kernel =
				static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel);
			ClutterRecord<K,V1,V2,V3> c=get(key);
			std::vector<std::pair<K, V1> > output;
			kernel->g_func(key, c.v1, c.v2, c.v3, &output);
			handleGeneratedInformation(key, output);
			*/
		} else { // reply current value to the source
			IterateKernel<K, V1, V3>* kernel =
				static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel);
			ClutterRecord<K,V1,V2,V3> c=get(key);
			//auto it = std::find_if(output.begin(), output.end(), [&](const std::pair<K, V2>& p) {
			//	return p.first == src;
			//});
			//if(it == output.end()) {
			//} else {

			//CHECK(it != output.end());
			V1 res = kernel->g_func(key, c.v1, c.v2, c.v3, src);
			std::vector<std::pair<K, V1> > temp;
			temp.emplace_back(src, res);

			handleGeneratedInformation(key, temp);
		}
	} else {
		VLOG(1) << "not local request";
		++pending_send_;
	}
}

template<class K, class V1, class V2, class V3>
inline void TypedGlobalTable<K, V1, V2, V3>::sendRequest(const K & local, const K & source)
{
	int shard = get_shard(local);
	StateTable<K, V1, V2, V3> *st = dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(shard));
	//IterateKernel<K, V1, V3>* kernel =
	//	static_cast<IterateKernel<K, V1, V3>*>(info().iterkernel);
	Marshal<K> *km = kmarshal();
	ValueRequest req;
	string temp;
	km->marshal(local, &temp);
	req.set_source(temp);
	//std::vector<K> outneighbors = kernel->get_keys(st->getF3(local));
	
	std::vector<K> inneighbors = st->get_ineighbor(local);
	//if(local == 248)
	 // VLOG(1)<<"PrepareSend "<<local<<" size: "<<inneighbors.size();
	for(auto& s : inneighbors) {
		if(s == source || s == local)
			continue;
		//if(local == 264 || local == 248)
		//	VLOG(1)<<"Send req from  "<<local<<" to "<<s<<" bp: "<<source;
		km->marshal(s, &temp);
		req.set_key(temp);
		int shard = get_shard(s);
		helper()->realSendRequest(owner(shard), req);
	}
}

// Store the given key-value pair in this hash. If 'k' has affinity for a
// remote thread, the application occurs immediately on the local host,
// and the update is queued for transmission to the owner.
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
//	DVLOG(3)<<"key: "<<k<<" delta: "<<v1<<" value: "<<v2<<"   "<<v3.size()<<" shard="<<shard<<" cl W"<<helper_id();
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
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
//	DVLOG(3)<<"key: "<<k<<" delta: "<<v1<<" value: "<<v2<<"   "<<v3.size()<<" shard="<<shard<<" rf W"<<helper_id();
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
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF1(k, v);
	}else{
		deltaT(shard)->update(k, v);
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF2(const K &k, const V2 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF2(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << helper_id();
	}else{
		VLOG(2) << "update F2 shard " << shard << " local? " << " : " << is_local_shard(shard)
							<< " : " << helper_id();
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF3(const K &k, const V3 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->updateF3(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << helper_id();
	}else{
		VLOG(2) << "update F3 shard " << shard << " local? " << " : " << is_local_shard(shard)
							<< " : " << helper_id();
	}
}

template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &from, const K &to, const V1 &v){ //3
	int shard = this->get_shard(to);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		//VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on local " << k;
		StateTable<K, V1, V2, V3> *st = dynamic_cast<StateTable<K, V1, V2, V3> *>(localT(shard));
		if(!st->accumulateF1(from, to, v)){  //send request to others
			sendRequest(to, from);
		}
		return false;
	}else{
		//VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on remote " << k;
		bufferGeneratedMessage(from, to, v);
		++pending_send_;
	}
	return true;
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &k, const V1 &v){ //3
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
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
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->accumulateF2(k, v); // Typetable

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << helper_id();
	}else{
		VLOG(2) << "accumulate F2 shard " << shard << " local? " << " : "
							<< is_local_shard(shard);
	}
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF3(const K &k, const V3 &v){
	int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<mutex> sl(trigger_get_mutex());
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif

	if(is_local_shard(shard)){
		localT(shard)->accumulateF3(k, v);

		//VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << helper_id();
	}else{
		VLOG(2) << "accumulate F3 shard " << shard << " local? " << " : "
							<< is_local_shard(shard) << " : " << helper_id();
	}
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
ClutterRecord<K, V1, V2, V3> TypedGlobalTable<K, V1, V2, V3>::get(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
	return localT(shard)->get(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V1 TypedGlobalTable<K, V1, V2, V3>::getF1(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
	return localT(shard)->getF1(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V2 TypedGlobalTable<K, V1, V2, V3>::getF2(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
	return localT(shard)->getF2(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V3 TypedGlobalTable<K, V1, V2, V3>::getF3(const K &k){
	int shard = this->get_shard(k);

	CHECK_EQ(is_local_shard(shard), true)<< "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
	std::lock_guard<std::recursive_mutex> sl(get_mutex());
#endif
	return localT(shard)->getF3(k);
}

template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::contains(const K &k){
	int shard = this->get_shard(k);

	if(is_local_shard(shard)){
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
		std::lock_guard<std::recursive_mutex> sl(get_mutex());
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
