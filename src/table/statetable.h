#ifndef TABLE_STATE_TABLE_H_
#define TABLE_STATE_TABLE_H_

#include "util/noncopyable.h"
#include "tbl_widget/IterateKernel.h"
//#include "tbl_widget/sharder.h"
//#include "tbl_widget/term_checker.h"
#include "table.h"
#include "local-table.h"
#include "TableHelper.h"
#include <random>
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <gflags/gflags.h>

#include "dbg/dbg.h"

namespace dsm {

static constexpr int SAMPLE_SIZE = 1000;

template<class K, class V1, class V2, class V3>
class StateTable:
		public LocalTable,
		public TypedTable<K, V1, V2, V3>,
		private noncopyable{
private:
#pragma pack(push, 1)
	struct Bucket{
		K k;
		V1 v1;
		V2 v2;
		V3 v3;
		V1 priority;
		std::unordered_map<K, V1> input; // values of in-neighbors
		K bp; // pointer to the best in-neighbor
		bool in_use;

		bool update_v1_with_input(const K& from, const V1& v, IterateKernel<K, V1, V3>* kernel);
		bool update_input(const K& from, const V1& v, IterateKernel<K, V1, V3>* kernel);
		void reset_v1_from_input(IterateKernel<K, V1, V3>* kernel);

		void reset_best_pointer(IterateKernel<K, V1, V3>* kernel);
	};
#pragma pack(pop)

public:
	typedef FileDecodeIterator<K, V1, V2, V3> FileUpdateDecoder;
	typedef NetDecodeIterator<K, V1> NetUpdateDecoder;

	struct Iterator: public TypedTableIterator<K, V1, V2, V3> {
		Iterator(StateTable<K, V1, V2, V3>& parent, bool bfilter) :
				pos(-1), parent_(parent){
			defaultv = static_cast<IterateKernel<K, V1, V3>*>(parent_.info_.iterkernel)->default_v();
			/* This filter is very important in large-scale experiment.
			 * If there is no such control, many useless parsing will occur.
			 * It will degrading the performance a lot
			 */
			if(static_cast<IterateKernel<K, V1, V3>*>(parent_.info_.iterkernel)->is_selective()){
				pick_pred=[&](int b){
					return parent_.buckets_[b].v1 != parent_.buckets_[b].v2;
				};
			}else{
				pick_pred=[&](int b){
					return parent_.buckets_[b].v1 != defaultv;
				};
			}
			if(bfilter){
				std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
				//DVLOG(1)<<"bunket size="<<parent_.buckets_.size();
				std::uniform_int_distribution<int> dist(0, parent_.buckets_.size() - 1);
				auto rand_num = [&](){return dist(gen);};

				//check if there is a change
				b_no_change = true;
				size_t end_i = std::min<size_t>(SAMPLE_SIZE, parent_.entries_ * 2);
				for(size_t i = 0; i < end_i && b_no_change; i++){
					int rand_pos = rand_num();
					while(!parent_.buckets_[rand_pos].in_use){
						rand_pos = rand_num();
					}

					b_no_change &= pick_pred(rand_pos);
				}
			}else{
				b_no_change = false;
			}
			Next();
		}
		Marshal<K>* kmarshal(){ return parent_.kmarshal(); }
		Marshal<V1>* v1marshal(){ return parent_.v1marshal(); }
		Marshal<V2>* v2marshal(){ return parent_.v2marshal(); }
		Marshal<V3>* v3marshal(){ return parent_.v3marshal(); }

		bool Next(){
			do{
				++pos;
			}while(pos < parent_.size_
					&& (!parent_.buckets_[pos].in_use || !pick_pred(pos)));
//			if(pos<parent_.size_)
//				VLOG_IF(0,value1()==defaultv)<<key()<<" : "<<value1()
//					<<" , "<<value2()<<" input: "<<parent_.buckets_[pos].input;
			return pos < parent_.size_;
		}

		bool done(){
			//cout<< "pos " << pos << "\tsize" << parent_.size_ << endl;
			return pos >= parent_.size_;
		}

		const K& key(){ return parent_.buckets_[pos].k; }
		V1& value1(){ return parent_.buckets_[pos].v1; }
		V2& value2(){ return parent_.buckets_[pos].v2; }
		V3& value3(){ return parent_.buckets_[pos].v3; }

		int pos;
		StateTable<K, V1, V2, V3> &parent_;
		bool b_no_change;
		V1 defaultv;
		std::function<bool(int)> pick_pred;
	};

	struct ScheduledIterator: public TypedTableIterator<K, V1, V2, V3> {
		ScheduledIterator(StateTable<K, V1, V2, V3>& parent, bool bfilter) :
				pos(-1), parent_(parent){

			b_no_change = true;
			const V1 defaultv=static_cast<IterateKernel<K, V1, V3>*>(parent_.info_.iterkernel)->default_v();
			
			if(static_cast<IterateKernel<K, V1, V3>*>(parent_.info_.iterkernel)->is_selective()){
				pick_pred=[&](int b){
					return parent_.buckets_[b].v1 != parent_.buckets_[b].v2;
				};
			}else{
				pick_pred=[&](int b){
					return parent_.buckets_[b].v1 != defaultv;
				};
			}

			if(parent_.entries_ <= SAMPLE_SIZE){
				//if table size is less than the sample set size, schedule them all
				scheduled_pos.reserve(parent_.entries_);
				int i;
				for(i = 0; i < parent_.size_; i++){
					if(parent_.buckets_[i].in_use){
						scheduled_pos.push_back(i);
						b_no_change = b_no_change && pick_pred(i);
					}
				}
				if(!bfilter) b_no_change = false;
			}else{
				//sample random pos, the sample reflect the whole data set more or less
				std::vector<int> sampled_pos;
/*
				//random number generator
				std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
				std::uniform_int_distribution<int> dist(0, parent_.buckets_.size() - 1);
				auto rand_num = [&](){return dist(gen);};
				int trials = 0;
				for(int i = 0; i < SAMPLE_SIZE; i++){
					int rand_pos = rand_num();
					trials++;
					while(!parent_.buckets_[rand_pos].in_use){
						rand_pos = rand_num();
						trials++;
					}
					sampled_pos.push_back(rand_pos);

					b_no_change = b_no_change && !pick_pred(rand_pos);
				}

				if(b_no_change && bfilter) return;
				if(!bfilter) b_no_change = false;

*/
				for(int i = 0; i < parent_.size_; i++){
					if(parent_.buckets_[i].in_use && pick_pred(i)){
						sampled_pos.push_back(i);
					}
				}
				int cut_index = parent_.entries_ * parent_.info_.schedule_portion;
				if(sampled_pos.size() <= cut_index+1){
					scheduled_pos=move(sampled_pos);
				}else{
					
					scheduled_pos = move(sampled_pos);
					partial_sort(scheduled_pos.begin(), scheduled_pos.begin()+(cut_index+1), scheduled_pos.end(), compare_priority(parent_));
					scheduled_pos.erase(scheduled_pos.bein()+cnt_index, scheduled_pos.end());

					/*
					//get the cut index, everything larger than the cut will be scheduled
					sort(sampled_pos.begin(), sampled_pos.end(), compare_priority(parent_));
					V1 threshold = parent_.buckets_[sampled_pos[cut_index]].priority;
					sampled_pos.clear();					
					VLOG(2) << "cut index " << cut_index << " threshold " << threshold << " pos "
										<< sampled_pos[cut_index] << " max "
										<< parent_.buckets_[sampled_pos[0]].v1;
					//Reserve parent_.size_*P instead of parent_.entries_*P because P is not correct portion
					//When P is smaller than real portion, reserving parent_.entries_*P slot may lead to an resize()
					//scheduled_pos.reserve(parent_.size_*cut_index/SAMPLE_SIZE);
					if(cut_index == 0 || parent_.buckets_[sampled_pos[0]].priority == threshold){
						//to avoid non eligible records
						for(int i = 0; i < parent_.size_; i++){
							if(!parent_.buckets_[i].in_use || !pick_pred(i)) continue;

							if(parent_.buckets_[i].priority >= threshold){// >=
								scheduled_pos.push_back(i);
							}
						}
					}else{
						for(int i = 0; i < parent_.size_; i++){
							if(!parent_.buckets_[i].in_use || !pick_pred(i)) continue;

							if(parent_.buckets_[i].priority > threshold){// >
								scheduled_pos.push_back(i);
							}
						}
					} */
				}
			}

			VLOG(2) << "table size " << parent_.buckets_.size() << " worker-id " << parent_.id()
						<< " scheduled " << scheduled_pos.size();
			Next();
		}

		Marshal<K>* kmarshal(){ return parent_.kmarshal(); }
		Marshal<V1>* v1marshal(){ return parent_.v1marshal(); }
		Marshal<V2>* v2marshal(){ return parent_.v2marshal(); }
		Marshal<V3>* v3marshal(){ return parent_.v3marshal(); }

		bool Next(){
			++pos;
			return pos < scheduled_pos.size();
		}

		bool done(){
			return pos >= scheduled_pos.size();
		}

		const K& key(){
			return parent_.buckets_[scheduled_pos[pos]].k;
		}
		V1& value1(){
			return parent_.buckets_[scheduled_pos[pos]].v1;
		}
		V2& value2(){
			return parent_.buckets_[scheduled_pos[pos]].v2;
		}
		V3& value3(){
			return parent_.buckets_[scheduled_pos[pos]].v3;
		}

		class compare_priority{
		public:
			StateTable<K, V1, V2, V3> &parent;

			compare_priority(StateTable<K, V1, V2, V3> &inparent) :
					parent(inparent){
			}

			bool operator()(const int a, const int b){
				return parent.buckets_[a].priority > parent.buckets_[b].priority;
				//return ((Accumulator<V1>*)parent.info_.accum)->priority(parent.buckets_[a].v1, parent.buckets_[a].v2)
				//> ((Accumulator<V1>*)parent.info_.accum)->priority(parent.buckets_[b].v1, parent.buckets_[b].v2);
			}
		};

		int pos;
		StateTable<K, V1, V2, V3> &parent_;
		double portion;
		std::vector<int> scheduled_pos;
		bool b_no_change;
		std::function<bool(int)> pick_pred;
	};

	//for termination check
	struct EntirePassIterator: public TypedTableIterator<K, V1, V2, V3>, public LocalTableIterator<
			K, V2> {
		EntirePassIterator(StateTable<K, V1, V2, V3>& parent) :
				pos(-1), parent_(parent){
			//Next();
			total = 0;
			pos = -1;
			defaultv = ((IterateKernel<K, V1, V3>*)parent_.info_.iterkernel)->default_v();
			Next();
		}

		Marshal<K>* kmarshal(){ return parent_.kmarshal(); }
		Marshal<V1>* v1marshal(){ return parent_.v1marshal(); }
		Marshal<V2>* v2marshal(){ return parent_.v2marshal(); }
		Marshal<V3>* v3marshal(){ return parent_.v3marshal(); }

		bool Next(){
			do{
				++pos;
			}while(pos < parent_.size_ && !parent_.buckets_[pos].in_use);
			total++;

			return pos<parent_.size_;
		}

		bool done(){
			//cout<< "entire pos " << pos << "\tsize" << parent_.size_ << endl;
			return pos >= parent_.size_;
		}

		V1 defaultV(){
			return defaultv;
		}

		const K& key(){ return parent_.buckets_[pos].k; }
		V1& value1(){ return parent_.buckets_[pos].v1; }
		V2& value2(){ return parent_.buckets_[pos].v2; }
		V3& value3(){ return parent_.buckets_[pos].v3; }
		const std::unordered_map<K, V1>& ineighbor(){ return parent_.buckets_[pos].input; }

		int pos;
		StateTable<K, V1, V2, V3> &parent_;
		int total;
		V1 defaultv;
	};

	struct Factory: public TableFactory{
		Table* New(){
			return new StateTable<K, V1, V2, V3>();
		}
	};

	// Construct a StateTable with the given initial size; it will be expanded as necessary.
	StateTable(int size = 1);

	void Init(const TableDescriptor* td){
		Table::Init(td);

		IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
		default_v = pk->default_v();
	}

	V1 getF1(const K& k);
	V2 getF2(const K& k);
	V3 getF3(const K& k);
	ClutterRecord<K, V1, V2, V3> get(const K& k);
	bool contains(const K& k);
	void put(const K& k, const V1& v1, const V2& v2, const V3& v3);
	void put(K&& k, V1&& v1, V2&& v2, V3&& v3);
	void updateF1(const K& k, const V1& v);
	void updateF2(const K& k, const V2& v);
	void updateF3(const K& k, const V3& v);
	void accumulateF1(const K& from, const K &to, const V1 &v);
	void accumulateF1(const K& k, const V1& v);
	void accumulateF2(const K& k, const V2& v);
	void accumulateF3(const K& k, const V3& v);
	bool remove(const K& k){
		LOG(FATAL)<< "Not implemented.";
		return false;
	}

	// XXX: evolving graph
	void change_graph(const K& k, const ChangeEdgeType& type, const V3& change);
	void add_ineighbor(const K& from, const K& to, const V1& v1);
	void update_ineighbor(const K& from, const K& to, const V1& v1);
	void remove_ineighbor(const K& from, const K& to);
	V1 get_ineighbor(const K& from, const K& to);
	void reset_best_pointer();
	void reset_best_pointer(const K& key);

	void resize(int64_t size);

	bool empty(){return size() == 0;}
	int64_t size(){return entries_;}
	int64_t capacity(){return size_;}

	void clear(){
		for (int64_t i = 0; i < size_; ++i){
			buckets_[i].in_use = 0;
		}
		entries_ = 0;
	}

	void reset(){
		buckets_.clear();
		size_=0;
		entries_=0;
		resize(1);
	}

	bool compare_priority(int i, int j){
		return buckets_[i].priority > buckets_[j].priority;
		//return ((Scheduler<K, V1>*)info_.scheduler)->priority(buckets_[i].k, buckets_[i].v1)
		//> ((Scheduler<K, V1>*)info_.scheduler)->priority(buckets_[j].k, buckets_[j].v1);
	}

	TableIterator *get_iterator(TableHelper* helper, bool bfilter){
		if(terminated_) return nullptr; //if get term signal, return nullptr to tell program terminate
		Iterator* iter = new Iterator(*this, false);
		return iter;
	}

	TableIterator *schedule_iterator(TableHelper* helper, bool bfilter){
		if(terminated_) return nullptr;
		ScheduledIterator* iter = new ScheduledIterator(*this, bfilter);
		return iter;
	}

	TableIterator *entirepass_iterator(TableHelper* helper){
		return new EntirePassIterator(*this);
	}

	void serializeToFile(TableCoder *out);
	void serializeToNet(KVPairCoder *out);
	void deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase);
	void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase);
	void serializeToSnapshot(const string& f, uint64_t* receives, uint64_t* updates, double* totalF2, uint64_t* defaultF2);

	Marshal<K>* kmarshal(){return ((Marshal<K>*)info_.key_marshal);}
	Marshal<V1>* v1marshal(){return ((Marshal<V1>*)info_.value1_marshal);}
	Marshal<V2>* v2marshal(){return ((Marshal<V2>*)info_.value2_marshal);}
	Marshal<V3>* v3marshal(){return ((Marshal<V3>*)info_.value3_marshal);}

private:
	uint32_t bucket_idx(const K& k){
		return hashobj_(k) % size_;
	}

	//get bucket for existed key, otherwise return -1
	int bucket_for_key(const K& k){
		int start = bucket_idx(k);
		int b = start;

		do{
			if(buckets_[b].in_use){
				if (buckets_[b].k == k){
					return b;
				}
			}else{
				return -1;
			}

			b = (b + 1) % size_;
		}while (b != start);

		return -1;
	}
	//get bucket to access a key.
	//when key exists return its bucket;
	//when key do not exist return a bucket to insert it (-1 when no place to insert)
	int bucket_for_access_key(const K& k){
		int start = bucket_idx(k);
		int b = start;

		do{
			if(!buckets_[b].in_use){
				return b;
			}else if(buckets_[b].k==k){
				return b;
			}
			b = (b + 1) % size_;
		}while (b != start);

		return -1;
	}

	std::vector<Bucket> buckets_;

	uint64_t entries_;
	uint64_t size_;

	std::hash<K> hashobj_;
	std::recursive_mutex m_; // mutex for resizing the table

private:
	// local sum, for termination checking
	V2 default_v;
	double total_curr_value;
	uint64_t total_curr_default;
	uint64_t total_receive; // number of updating delta
	uint64_t total_updates; // number of merging delta into value

	void update_local_sum(const V2& oldV, const V2& newV){
		if(oldV == default_v)
			--total_curr_default;
		else
			total_curr_value -= oldV;
		if(newV == default_v)
			++total_curr_default;
		else
			total_curr_value += newV;
	}
	void update_local_sum(const V2& newV){
		if(newV == default_v)
			++total_curr_default;
		else
			total_curr_value += newV;
	}
};

template<class K, class V1, class V2, class V3>
StateTable<K, V1, V2, V3>::StateTable(int size) :
	buckets_(0), entries_(0), size_(0), total_curr_value(0.0), total_curr_default(0), total_receive(0), total_updates(0)
{
	clear();

	VLOG(2) << "new statetable size " << size;
	resize(size);
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::serializeToFile(TableCoder *out){
	Iterator *i = (Iterator*)get_iterator(nullptr, false);
	string k, v1, v2, v3;
	while(!i->done()){
		k.clear();
		v1.clear();
		v2.clear();
		v3.clear();
		DVLOG(2)<<i->pos<<": k="<<i->key()<<" v1="<<i->value1()<<" v2="<<i->value2();
		((Marshal<K>*)info_.key_marshal)->marshal(i->key(), &k);
		// DVLOG(1)<<"k="<<i->key()<<" - "<<k;
		((Marshal<V1>*)info_.value1_marshal)->marshal(i->value1(), &v1);
		// DVLOG(1)<<"v1="<<i->value1()<<" - "<<v1;
		((Marshal<V2>*)info_.value2_marshal)->marshal(i->value2(), &v2);
		// DVLOG(1)<<"v2="<<i->value2()<<" - "<<v3;
		// ((Marshal<V3>*)info_.value3_marshal)->marshal(i->value3(), &v3);
		// DVLOG(1)<<"v3="<<i->value3()<<" - "<<v3;
		out->WriteEntryToFile(k, v1, v2, v3);
		i->Next();
	}
	delete i;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::serializeToNet(KVPairCoder *out){
	Iterator *i = (Iterator*)get_iterator(nullptr, false);
	string k, v1;
	// TODO: change latter to add source
	while(!i->done()){
		k.clear();
		v1.clear();
		((Marshal<K>*)info_.key_marshal)->marshal(i->key(), &k);
		((Marshal<V1>*)info_.value1_marshal)->marshal(i->value1(), &v1);
		out->WriteEntryToNet(k, v1);
		i->Next();
	}
	delete i;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase){
	FileUpdateDecoder* it = static_cast<FileUpdateDecoder*>(itbase);
	K k;
	V1 v1;
	V2 v2;
	V3 v3;
	string kt, v1t, v2t, v3t;

	it->clear();
	while(in->ReadEntryFromFile(&kt, &v1t, &v2t, &v3t)){
		((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
		((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
		((Marshal<V2>*)info_.value2_marshal)->unmarshal(v2t, &v2);
		((Marshal<V3>*)info_.value3_marshal)->unmarshal(v3t, &v3);
		it->append(k, v1, v2, v3);
	}
	it->rewind();
	return;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase){
	NetUpdateDecoder* it = static_cast<NetUpdateDecoder*>(itbase);
	K k;
	V1 v1;
	string kt, v1t;
	// seems never be called
	it->clear();
	while(in->ReadEntryFromNet(&kt, &v1t)){
		((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
		((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
		it->append(k, v1);
	}
	it->rewind();
	return;
}

//it can also be used to generate snapshot, but currently in order to measure the performance we skip this step, 
//but focus on termination check
template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::serializeToSnapshot(const string& f,
		uint64_t* receives, uint64_t* updates, double* totalF2, uint64_t* defaultF2){
	//total_curr_value = 0;
	//EntirePassIterator* entireIter = new EntirePassIterator(*this);
	//total_curr_value = static_cast<double>(((TermChecker<K, V2>*)info_.termchecker)
	//		->estimate_prog(entireIter));
	//delete entireIter;
	*receives = total_receive;
	*updates = total_updates;
	*totalF2 = total_curr_value;
	*defaultF2 = total_curr_default;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::resize(int64_t size){
	CHECK_GT(size, 0);
	if(size_ == size)
		return;

	std::vector<Bucket> old_b = move(buckets_);
	int old_entries = entries_;

	// LOG(INFO) << "Rehashing.vd.. " << entries_ << " : " << size_ << " -> " << size;

	buckets_.resize(size);
	size_ = size;
	clear();
	// LOG(INFO) << "Rehashing... " << entries_ << " : " << size_ << " -> " << size;
	for(int i = 0; i < old_b.size(); ++i){
		if(old_b[i].in_use){
			put(move(old_b[i].k), move(old_b[i].v1), move(old_b[i].v2), move(old_b[i].v3));
			// LOG(INFO)<< "copy: " << old_b[i].k;
		}
	}
	CHECK_EQ(old_entries, entries_)<<getcallstack();
}

template<class K, class V1, class V2, class V3>
bool StateTable<K, V1, V2, V3>::contains(const K& k){
	return bucket_for_key(k) != -1;
}

template<class K, class V1, class V2, class V3>
V1 StateTable<K, V1, V2, V3>::getF1(const K& k){
	int b = bucket_for_key(k);
	//The following key display is a hack hack hack and only yields valid
	//results for ints.  It will display nonsense for other types.
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	return buckets_[b].v1;
}

template<class K, class V1, class V2, class V3>
V2 StateTable<K, V1, V2, V3>::getF2(const K& k){
	int b = bucket_for_key(k);
	//The following key display is a hack hack hack and only yields valid
	//results for ints.  It will display nonsense for other types.
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	return buckets_[b].v2;
}

template<class K, class V1, class V2, class V3>
V3 StateTable<K, V1, V2, V3>::getF3(const K& k){
	int b = bucket_for_key(k);
	//The following key display is a hack hack hack and only yields valid
	//results for ints.  It will display nonsense for other types.
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	return buckets_[b].v3;
}

template<class K, class V1, class V2, class V3>
ClutterRecord<K, V1, V2, V3> StateTable<K, V1, V2, V3>::get(const K& k){
	int b = bucket_for_key(k);
	//The following key display is a hack hack hack and only yields valid
	//results for ints.  It will display nonsense for other types.
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	return ClutterRecord<K, V1, V2, V3>(k, buckets_[b].v1, buckets_[b].v2, buckets_[b].v3);
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::updateF1(const K& k, const V1& v){
	int b = bucket_for_key(k);

	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	buckets_[b].v1 = v;
	buckets_[b].priority = 0;	//didn't use priority function, assume the smallest priority is 0
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::updateF2(const K& k, const V2& v){
	int b = bucket_for_key(k);

	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	update_local_sum(buckets_[b].v2, v);
	buckets_[b].v2 = v;

	++total_updates;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::updateF3(const K& k, const V3& v){
	int b = bucket_for_key(k);

	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	buckets_[b].v3 = v;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::accumulateF1(const K& from, const K &to, const V1 &v){
	int b = bucket_for_key(to);
	IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
	buckets_[b].update_v1_with_input(from, v, pk);
	pk->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1, buckets_[b].v3);

	++total_receive;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::accumulateF1(const K& k, const V1& v){
	int b = bucket_for_key(k);
	//cout << "accumulate " << k << "\t" << v << endl;
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) <<">"<< "key: "<<k;
	IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
	pk->accumulate(buckets_[b].v1, v);
	pk->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1, buckets_[b].v3);

	++total_receive;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::accumulateF2(const K& k, const V2& v){
	int b = bucket_for_key(k);

	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";
	V2 old = buckets_[b].v2;
	static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel)->accumulate(buckets_[b].v2, v);
	update_local_sum(old, v);

	++total_updates;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::accumulateF3(const K& k, const V3& v){

}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::put(const K& k, const V1& v1, const V2& v2, const V3& v3){
	DVLOG(2)<<"lock in put ";//<<getcallstack();
	std::lock_guard<std::recursive_mutex> gl(m_);
	int b=bucket_for_access_key(k);
	if(b==-1 /* || (!buckets_[b].in_use && entries_ >= size_*kLoadFactor) */){
		//doesn't consider loadfactor, the tablesize is pre-defined
		VLOG(2) << "resizing... " << size_ << " : " << (int)(1 + size_ * 2)
				<< " entries " << entries_;
		resize(1 + size_ * 2);
		b=bucket_for_access_key(k);
	}
//	DVLOG(3)<<"key: "<<k<<" delta: "<<v1<<" value: "<<v2<<"   "<<v3.size();
	if(buckets_[b].in_use == false){
		buckets_[b].in_use = true;
		++entries_;
		update_local_sum(v2);
	}else{
		update_local_sum(buckets_[b].v2, v2);

	}
	buckets_[b].k = k;
	buckets_[b].v1 = v1;
	buckets_[b].v2 = v2;
	buckets_[b].v3 = v3;
	static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel)->priority(
			buckets_[b].priority, buckets_[b].v2, buckets_[b].v1, buckets_[b].v3);
	DVLOG(2)<<"unlock in put";
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::put(K&& k, V1&& v1, V2&& v2, V3&& v3){
	DVLOG(2)<<"lock in put& ";//<<getcallstack();
	std::lock_guard<std::recursive_mutex> gl(m_);
	int b=bucket_for_access_key(k);
	if(b==-1 /* || (!buckets_[b].in_use && entries_ >= size_*kLoadFactor) */){
		//doesn't consider loadfactor, the tablesize is pre-defined
		VLOG(2) << "resizing... " << size_ << " : " << (int)(1 + size_ * 2)
				<< " entries " << entries_;
		resize(1 + size_ * 2);
		b=bucket_for_access_key(k);
	}
	// DVLOG(3)<<"key: "<<k<<" delta: "<<v1<<" value: "<<v2<<"   "<<v3.size()<<" b "<<b;
	if(buckets_[b].in_use == false){
		buckets_[b].in_use = true;
		++entries_;
		update_local_sum(v2);
	}else{
		update_local_sum(buckets_[b].v2, v2);
	}
	buckets_[b].k = std::forward<K>(k);
	buckets_[b].v1 = std::forward<V1>(v1);
	buckets_[b].v2 = std::forward<V2>(v2);
	buckets_[b].v3 = std::forward<V3>(v3);
	static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel)->priority(
			buckets_[b].priority, buckets_[b].v2, buckets_[b].v1, buckets_[b].v3);
	DVLOG(2)<<"unlock in put&";
}

// XXX: for evolving graph:
template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::change_graph(const K& k, const ChangeEdgeType& type, const V3& change){
	DVLOG(2)<<"lock in change";
	std::lock_guard<std::recursive_mutex> gl(m_);
	int b=bucket_for_key(k);
	Bucket& bk=buckets_[b];
	switch(type){
	case ChangeEdgeType::ADD:
		//VLOG(1)<<k<<"\t"<<change[0].end<<" - "<<change[0].weight<<" | "<<
		//	bk.k<<" "<<bk.v1<<" "<<bk.v2<<" "<<bk.v3.size()<<" "<<bk.input.size()<<" "<<bk.priority;
		bk.v3.push_back(change.front());
		break;
	case ChangeEdgeType::REMOVE:
		bk.v3.erase(std::find(bk.v3.begin(), bk.v3.end(), change.front()));
		break;
	case ChangeEdgeType::INCREASE:
		// compare with key, assign with <key, value> pair
		*std::find(bk.v3.begin(), bk.v3.end(), change.front())=change.front();
		break;
	case ChangeEdgeType::DECREASE:
		*std::find(bk.v3.begin(), bk.v3.end(), change.front())=change.front();
		break;
	}
	DVLOG(2)<<"unlock in change";
}

template<class K, class V1, class V2, class V3>
bool StateTable<K, V1, V2, V3>::Bucket::update_v1_with_input(
	const K& from, const V1& v, IterateKernel<K, V1, V3>* kernel)
{
//	VLOG(0)<<getcallstack();
//	VLOG_IF(0, (v1==kernel->default_v()))<<"at "<<k<<": "<<v1<<", "<<v2<<" get: ("<<from<<": "<<v<<") input: "<<input;

	bool good_change = update_input(from, v, kernel);
	if(v1 == kernel->default_v()){
		v1 = v;
		bp = from;
	}else if(good_change || v1 ==kernel->default_v()){
		if(kernel->better(v, v1)){
			//kernel->accumulate(v1, v);
			v1 = v;
			bp = from;
		}
//		VLOG(1)<<" good message from "<<from<<" to "<<k<<" old="<<old<<" new="<<v
//				<<" bp-old="<<oldbp<<" bp-new="<<bp<<" best-old="<<oldBest<<" best-new="<<v1<<" v="<<v2;
//		VLOG(1)<<"   "<<input;
	}else{
		if(bp == from){
			reset_v1_from_input(kernel);
		}
//		VLOG(1)<<" -bad message from "<<from<<" to "<<k<<" old="<<old<<" new="<<v
//				<<" bp-old="<<oldbp<<" bp-new="<<bp<<" best-old="<<oldBest<<" best-new="<<v1<<" v="<<v2;
//		VLOG(1)<<"   "<<input;
	}
	return good_change;
}

template<class K, class V1, class V2, class V3>
bool StateTable<K, V1, V2, V3>::Bucket::update_input(
	const K& from, const V1& v, IterateKernel<K, V1, V3>* kernel)
{
	// if from is not recorded, the default value is considered
	auto it=input.find(from);
	const V1& old = it==input.end() ? kernel->default_v() : it->second;	// add case
	bool better = kernel->better(v, old);
	//if(it!=input.end() || v != kernel->default_v())
		input[from] = v;	// modify & add case
//	if(it!=input.end() && v==kernel->default_v())	// remove case
//		input.erase(from);
	return better;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::Bucket::reset_v1_from_input(IterateKernel<K, V1, V3>* kernel)
{
	if(input.empty())
		return;
	auto it = input.begin();
	V1 temp = it->second;
	K b = it->first;
	while(++it != input.end()){
		if(kernel->better(it->second, temp)){
			temp = it->second;
			b = it->first;
		}
	}
	v1 = temp;
	bp = b;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::Bucket::reset_best_pointer(IterateKernel<K, V1, V3>* kernel)
{
	if(input.empty()){
		v1 = kernel->default_v();
		bp = K();
		return;
	}
	auto it = input.begin();
	auto best = input.begin();
	for(++it; it!=input.end(); ++it){
		if(kernel->better(it->second, best->second)){
			best = it;
		}
	}
	bp=best->first;
//	VLOG_IF(0, bp != bp1)<<"bp on "<<k
//			<<" ( "<<bp1<<", "<<bp<<") value: "<<temp
//			<<" size: "<<input.size()<<" content: "<<input;
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::add_ineighbor(const K& from, const K& to, const V1& v1){
	DVLOG(2)<<"lock in add";
	std::lock_guard<std::recursive_mutex> gl(m_);
	int b=bucket_for_access_key(to);
	if(b==-1){
		//doesn't consider loadfactor, the tablesize is pre-defined
		VLOG(2) << "resizing... " << size_ << " : " << (int)(1 + size_ * 2)
				<< " entries " << entries_;
		resize(1 + size_ * 2);
		b=bucket_for_access_key(to);
		CHECK_NE(b, -1) << "failed re-access... size=" << size_ << " entries=" << entries_
				<<" , "<<from <<"-"<<to<<" v="<<v1;
	}
	if(buckets_[b].in_use == false){
		buckets_[b].in_use = true;
		++entries_;
	}
	buckets_[b].k = to;
	buckets_[b].input[from] = v1;
	DVLOG(2)<<"unlock in add";
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::update_ineighbor(const K& from, const K& to, const V1& v){
	int b = bucket_for_key(to);
	CHECK_NE(b, -1) << "No entry for requested key <" << *((int*)&to) << ">";
	IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
	buckets_[b].update_input(from, v, pk);
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::remove_ineighbor(const K& from, const K& to){
	int b = bucket_for_key(to);
	CHECK_NE(b, -1) << "No entry for requested key <" << *((int*)&to) << ">";
	buckets_[b].input.erase(from);
}

template<class K, class V1, class V2, class V3>
V1 StateTable<K, V1, V2, V3>::get_ineighbor(const K& from, const K& to){
	int b = bucket_for_key(to);
	CHECK_NE(b, -1) << "No entry for requested key <" << *((int*)&to) << ">";
	return buckets_[b].input[from];
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::reset_best_pointer(){
	std::lock_guard<std::recursive_mutex> lg(m_);
	IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
	VLOG(1)<<"reset bp on state table "<<helper_id();
	for(uint64_t i=0;i<size_;++i){
		if(buckets_[i].in_use){
			buckets_[i].reset_best_pointer(pk);
		}
	}
}

template<class K, class V1, class V2, class V3>
void StateTable<K, V1, V2, V3>::reset_best_pointer(const K& key){
	std::lock_guard<std::recursive_mutex> lg(m_);
	IterateKernel<K, V1, V3>* pk = static_cast<IterateKernel<K, V1, V3>*>(info_.iterkernel);
	int b = bucket_for_key(key);
	CHECK_NE(b, -1) << "No entry for requested key <" << *((int*)&key) << ">";
	buckets_[b].reset_best_pointer(pk);
}


} //namespace dsm
#endif /* TABLE_STATE_TABLE_H_ */
