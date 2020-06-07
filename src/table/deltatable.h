/*
 * deltatable.h
 *
 *  Created on: Aug 4, 2011
 *      Author: yzhang
 */

#ifndef DELTATABLE_H_
#define DELTATABLE_H_

#include "util/noncopyable.h"
#include "msg/message.pb.h"
#include "table.h"
#include "local-table.h"

namespace dsm {

template<class K, class V1, class D>
class DeltaTable:
		public LocalTable,
		public PTypedTable<K, V1, D>,
		private noncopyable{
private:
#pragma pack(push, 1)
	struct Bucket{
		K k;
		V1 v1;
		bool in_use;
	};
	#pragma pack(pop)

public:
	typedef FileDecodeIterator<K, V1, int, int> FileUpdateDecoder;
	typedef NetDecodeIterator<K, V1> NetUpdateDecoder;

	struct Iterator: public PTypedTableIterator<K, V1> {
		Iterator(DeltaTable<K, V1, D>& parent) :
				pos(-1), parent_(parent){
			Next();
		}

		Marshal<K>* kmarshal(){
			return parent_.kmarshal();
		}
		Marshal<V1>* v1marshal(){
			return parent_.v1marshal();
		}

		bool Next(){
			do{
				++pos;
			}while(pos < parent_.size_ && !parent_.buckets_[pos].in_use);

			return pos<parent_.size_;
		}

		bool done(){
			return pos >= parent_.size_;
		}

		const K& key(){
			return parent_.buckets_[pos].k;
		}
		V1& value1(){
			return parent_.buckets_[pos].v1;
		}

		int pos;
		DeltaTable<K, V1, D> &parent_;
	};

	struct Factory: public TableFactory{
		Table* New(){
			return new DeltaTable<K, V1, D>();
		}
	};

	// Construct a DeltaTable with the given initial size; it will be expanded as necessary.
	DeltaTable(int size = 1);
	~DeltaTable(){
	}

	void Init(const TableDescriptor* td){
		Table::Init(td);
	}

	V1 get(const K& k);
	bool contains(const K& k);
	void put(const K& k, const V1& v1);
	void put(K&& k, V1&& v1);
	void update(const K& k, const V1& v);
	void accumulate(const K& k, const V1& v);
	bool remove(const K& k){
		LOG(FATAL)<< "Not implemented.";
		return false;
	}

	void resize(int64_t size);

	bool empty(){return size() == 0;}
	int64_t size(){return entries_;}
	int64_t capacity(){return size_;}

	void clear(){
		for (int i = 0; i < size_; ++i){buckets_[i].in_use = 0;}
		entries_ = 0;
	}

	void reset(){
		buckets_.clear();
		size_ = 0;
		entries_ = 0;
		resize(1);
	}

	TableIterator *get_iterator(TableHelper* helper, bool bfilter){
		return new Iterator(*this);
	}

	TableIterator *schedule_iterator(TableHelper* helper, bool bfilter){
		return nullptr;
	}

	TableIterator *entirepass_iterator(TableHelper* helper){
		return nullptr;
	}

	void dump(std::ofstream& fout);
	void restore(std::ifstream& fin);

	void serializeToFile(TableCoder *out);
	void serializeToNet(KVPairCoder *out);
	void deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase);
	void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase);
	void serializeToSnapshot(const string& f, int64_t* updates, double* totalF2){return;}

	Marshal<K>* kmarshal(){return ((Marshal<K>*)info_.key_marshal);}
	Marshal<V1>* v1marshal(){return ((Marshal<V1>*)info_.value1_marshal);}

private:
	uint32_t bucket_idx(K k){
		CHECK_NE(size_,0)<<"size of deltable is 0";
		return static_cast<uint32_t>(hashobj_(k) % size_);
	}

	int bucket_for_key(const K& k){
		int start = bucket_idx(k);
		int b = start;

		do{
			if (buckets_[b].in_use){
				if (buckets_[b].k == k){
					return b;
				}
			} else{
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

	int64_t entries_;
	int64_t size_;

	std::hash<K> hashobj_;
};

template<class K, class V1, class D>
DeltaTable<K, V1, D>::DeltaTable(int size):buckets_(0), entries_(0), size_(0)
{
	clear();
	resize(size);
}

template<class K, class V1, class D>
inline void DeltaTable<K, V1, D>::dump(std::ofstream& fout)
{
	Iterator* i = (Iterator*)get_iterator(nullptr, false);
	fout << "delta:" << shard() << ",size:" << size() << "\n";
	while(!i->done()){
		DVLOG(2) << i->pos << ": k=" << i->key() << " v1=" << i->value1();
		fout << i->key() << ',' << i->value1() << '\n';
		i->Next();
	}
	delete i;
}

template<class K, class V1, class D>
inline void DeltaTable<K, V1, D>::restore(std::ifstream& fin)
{
	std::string line;
	std::getline(fin, line);
	size_t p = line.find(',', 6);
	CHECK_EQ(std::string("delta:")+std::to_string(shard()), line.substr(0, p))
		<< " header of delta table is not correct: " << line;
	clear();
	StringMarshal<K> km;
	StringMarshal<V1> vm;
	K k;
	V1 v1;
	int n = stoi(line.substr(p + 5));
	while(n > 0){
		std::getline(fin, line);
		p = line.find(',');
		km.unmarshal(line.substr(0, p), &k);
		vm.unmarshal(line.substr(p + 1), &v1);
		put(k, v1);
		n--;
	}
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::serializeToFile(TableCoder *out){
	Iterator *i = (Iterator*)get_iterator(nullptr, false);
	string k, v1;
	while(!i->done()){
		k.clear();
		v1.clear();
		DVLOG(1)<<i->pos<<": k="<<i->key()<<" v1="<<i->value1();
		((Marshal<K>*)info_.key_marshal)->marshal(i->key(), &k);
//		DVLOG(1)<<"k="<<i->key()<<" - "<<k;
		((Marshal<V1>*)info_.value1_marshal)->marshal(i->value1(), &v1);
//		DVLOG(1)<<"v1="<<i->value1()<<" - "<<v1;
		out->WriteEntryToFile(k, v1, "", "");
		i->Next();
	}
	delete i;
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::serializeToNet(KVPairCoder *out){
	Iterator *i = (Iterator*)get_iterator(nullptr, false);
	string k, v1;
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

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::deserializeFromFile(TableCoder *in, DecodeIteratorBase *itbase){
	FileUpdateDecoder* it = static_cast<FileUpdateDecoder*>(itbase);
	K k;
	V1 v1;
	string kt, v1t, v2t, v3t;

	it->clear();
	while(in->ReadEntryFromFile(&kt, &v1t, &v2t, &v3t)){
		((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
		((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
		it->append(k, v1, 0, 0);
	}
	it->rewind();
	return;
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *itbase){
	NetUpdateDecoder* it = static_cast<NetUpdateDecoder*>(itbase);
	K k;
	V1 v1;
	string kt, v1t;

	it->clear();
	while(in->ReadEntryFromNet(&kt, &v1t)){
		((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
		((Marshal<V1>*)info_.value1_marshal)->unmarshal(v1t, &v1);
		it->append(k, v1);
	}
	it->rewind();
	return;
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::resize(int64_t size){
	CHECK_GT(size, 0);
	if(size_ == size)
		return;

	std::vector<Bucket> old_b = move(buckets_);
	int64_t old_entries = entries_;

	DVLOG(2) << "Rehashing... " << entries_ << " : " << size_ << " -> " << size;

	buckets_.resize(size);
	size_ = size;
	clear();

	for(int i = 0; i < old_b.size(); ++i){
		if(old_b[i].in_use){
			put(old_b[i].k, old_b[i].v1);
		}
	}

	CHECK_EQ(old_entries, entries_);
}

template<class K, class V1, class D>
bool DeltaTable<K, V1, D>::contains(const K& k){
	return bucket_for_key(k) != -1;
}

template<class K, class V1, class D>
V1 DeltaTable<K, V1, D>::get(const K& k){
	int b = bucket_for_key(k);
	//The following key display is a hack hack hack and only yields valid
	//results for ints.  It will display nonsense for other types.
	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	return buckets_[b].v1;
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::update(const K& k, const V1& v){
	int b = bucket_for_key(k);

	CHECK_NE(b, -1)<< "No entry for requested key <" << *((int*)&k) << ">";

	buckets_[b].v1 = v;
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::accumulate(const K& k, const V1& v){
	int b = bucket_for_key(k);

	if(b == -1){
		put(k, v);
	}else{
		//((IterateKernel<K, V1, D>*)info_.iterkernel)->accumulate(&buckets_[b].v1, v);
		((IterateKernel<K, V1, D>*)info_.iterkernel)->accumulate(buckets_[b].v1, v);
	}
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::put(const K& k, const V1& v1){
	int b=bucket_for_access_key(k);
	if(b==-1){
		resize(1 + size_ * 2);
		b=bucket_for_access_key(k);
	}

	VLOG(3) << "put " << k << "," << v1 << " into deltatable";
	if(buckets_[b].in_use!=true){
		buckets_[b].in_use = true;
		buckets_[b].k = k;
	}
	buckets_[b].v1 = v1;
	++entries_;
	return;

//	int start = bucket_idx(k);
//	int b = start;
//	bool found = false;
//
//	do{
//		if(!buckets_[b].in_use){
//			break;
//		}
//
//		if(buckets_[b].k == k){
//			found = true;
//			break;
//		}
//
//		b = (b + 1) % size_;
//	}while(b != start);
//
//	// Inserting a new entry:
//	if(!found){
//		if(entries_ > size_ * kLoadFactor){
//			resize((int)(1 + size_ * 2));
//			put(k, v1);
//		}else{
//			buckets_[b].in_use = 1;
//			buckets_[b].k = k;
//			buckets_[b].v1 = v1;
//			++entries_;
//		}
//	}else{
//		// Replacing an existing entry
//		buckets_[b].v1 = v1;
//	}
}

template<class K, class V1, class D>
void DeltaTable<K, V1, D>::put(K&& k, V1&& v1){
	int b=bucket_for_access_key(k);
	if(b==-1){
		resize(1 + size_ * 2);
		b=bucket_for_access_key(k);
	}

	VLOG(3) << "put " << k << "," << v1 << " into deltatable";
	if(buckets_[b].in_use!=true){
		buckets_[b].in_use = true;
		buckets_[b].k = std::forward<K>(k);
	}
	buckets_[b].v1 = std::forward<V1>(v1);
	++entries_;
}

} //namespace dsm

#endif /* DELTATABLE_H_ */
