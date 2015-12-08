#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H
//#include "util/common.h"
#include "util/file.h"
#include "worker/worker.pb.h"

#include "sharder.h"
//#include "sharder.hpp"
#include "term_checker.h"
//#include "term_checker_impl.hpp"
#include "table_descriptor.h"
#include "table_iterator.h"
#include "trigger.h"

#include <string>
#include <vector>
#include <utility>
#include <boost/thread.hpp>


#define FETCH_NUM (2048)

DECLARE_double(termcheck_threshold);

namespace dsm {

struct TableBase;
struct Table;

template <class K, class V1, class V2, class V3>
class TypedGlobalTable;


class TableData;

// This interface is used by global tables to communicate with the outside
// world and determine the current state of a computation.
struct TableHelper {
  virtual int id() const = 0;
  virtual int epoch() const = 0;
  virtual int peer_for_shard(int table, int shard) const = 0;
  virtual void HandlePutRequest() = 0;
  virtual void FlushUpdates() = 0;
  virtual void SendTermcheck(int index, long updates, double current) = 0;
  virtual ~TableHelper(){}
};

template<class K, class V1, class V2, class V3>
struct ClutterRecord{
	K k;
	V1 v1;
	V2 v2;
	V3 v3;

	ClutterRecord() : k(), v1(), v2(), v3(){}

	ClutterRecord(const K& __a, const V1& __b, const V2& __c, const V3& __d):
		k(__a), v1(__b), v2(__c), v3(__d){}

	template<class K1, class U1, class U2, class U3>
	ClutterRecord(const ClutterRecord<K1, U1, U2, U3>& __p):
		k(__p.k), v1(__p.v1), v2(__p.v2), v3(__p.v3){}

	ostream& operator<<(ostream& out){
		return out << k << "\t" << v1 << "|" << v2 << "|" << v3;
	}
};

//
//template <class K, class V>
//struct LocalTableIterator {
//    virtual const K& key() = 0;
//    virtual V& value2() = 0;
//    virtual bool done() = 0;
//    virtual bool Next() = 0;
//    virtual V defaultV() = 0;
//    virtual ~LocalTableIterator(){}
//};

// Each table is associated with a single accumulator.  Accumulators are
// applied whenever an update is supplied for an existing key-value cell.

// Commonly used accumulation and sharding operators.
/*
template <class V>
struct Accumulators {
  struct Min : public Accumulator<V> {
    void accumulate(V* a, const V& b) { *a = std::min(*a, b); }
    V priority(const V& delta, const V& state) {return state - std::min(state, delta);}
  };

  struct Max : public Accumulator<V> {
    void accumulate(V* a, const V& b) { *a = std::max(*a, b); }
    V priority(const V& delta, const V& state) {return std::max(state, delta) - state;}
  };

  struct Sum : public Accumulator<V> {
    void accumulate(V* a, const V& b) { *a = *a + b; }
    V priority(const V& delta, const V& state) {return delta;}
  };
};
*/


struct TableFactory{
	virtual TableBase* New() = 0;
	virtual ~TableFactory(){}
};

struct Table{
	virtual const TableDescriptor& info() const = 0;
	virtual TableDescriptor& mutable_info() = 0;
	virtual int id() const = 0;
	virtual int num_shards() const = 0;
	virtual ~Table(){}
};

struct UntypedTable{
	virtual bool contains_str(const StringPiece& k) = 0;
	virtual std::string get_str(const StringPiece &k) = 0;
	virtual void update_str(const StringPiece &k, const StringPiece &v1, const StringPiece &v2,
			const StringPiece &v3) = 0;
	virtual ~UntypedTable(){}
};

//struct TableIterator {
//  virtual void key_str(std::string *out) = 0;
//  virtual void value1_str(std::string *out) = 0;
//  virtual void value2_str(std::string *out) = 0;
//  virtual void value3_str(std::string *out) = 0;
//  virtual bool done() = 0;
//  virtual bool Next() = 0;
//  virtual ~TableIterator(){}
//};

// Methods common to both global and local table views.
class TableBase: public Table{
public:
	typedef TableIterator Iterator;
	virtual void Init(const TableDescriptor* info){
		info_ = *info;
		terminated_ = false;
		CHECK(info_.key_marshal != nullptr);
		CHECK(info_.value1_marshal != nullptr);
		CHECK(info_.value2_marshal != nullptr);
		CHECK(info_.value3_marshal != nullptr);
	}

	const TableDescriptor& info() const{
		return info_;
	}
	TableDescriptor& mutable_info(){
		return info_;
	}
	int id() const{
		return info().table_id;
	}
	int num_shards() const{
		return info().num_shards;
	}

	TableHelper *helper(){
		return info().helper;
	}
	int helper_id(){
		return helper()->id();
	}

	int num_triggers(){
		return info_.triggers.size();
	}
	TriggerBase *trigger(int idx){
		return info_.triggers[idx];
	}

	TriggerID register_trigger(TriggerBase *t){
		if(helper()){
			t->helper = helper();
		}
		t->table = this;
		t->triggerid = info_.triggers.size();

		info_.triggers.push_back(t);
		return t->triggerid;
	}

	void set_helper(TableHelper *w){
		for(int i = 0; i < info_.triggers.size(); ++i){
			trigger(i)->helper = w;
		}

		info_.helper = w;
	}

	void terminate(){
		terminated_ = true;
	}

protected:
	TableDescriptor info_;
	bool terminated_;
};

// Key/value typed interface.
template<class K, class V1, class V2, class V3>
class TypedTable: virtual public UntypedTable{
public:
	virtual bool contains(const K &k) = 0;
	virtual V1 getF1(const K &k) = 0;
	virtual V2 getF2(const K &k) = 0;
	virtual V3 getF3(const K &k) = 0;
	virtual ClutterRecord<K, V1, V2, V3> get(const K &k) = 0;
	virtual void put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3) = 0;
	virtual void updateF1(const K &k, const V1 &v) = 0;
	virtual void updateF2(const K &k, const V2 &v) = 0;
	virtual void updateF3(const K &k, const V3 &v) = 0;
	virtual void accumulateF1(const K &k, const V1 &v) = 0; //4 TypeTable
	virtual void accumulateF2(const K &k, const V2 &v) = 0;
	virtual void accumulateF3(const K &k, const V3 &v) = 0;
	virtual bool remove(const K &k) = 0;

	// Default specialization for untyped methods
	virtual bool contains_str(const StringPiece& s){
		K k;
		kmarshal()->unmarshal(s, &k);
		return contains(k);
	}

	virtual std::string get_str(const StringPiece &s){
		K k;
		std::string f1, f2, f3;

		kmarshal()->unmarshal(s, &k);
		v1marshal()->marshal(getF1(k), &f1);
		v2marshal()->marshal(getF2(k), &f2);
		v3marshal()->marshal(getF3(k), &f3);
		return f1 + f2 + f3;
	}

	virtual void update_str(const StringPiece& kstr, const StringPiece &vstr1,
			const StringPiece &vstr2, const StringPiece &vstr3){
		K k;
		V1 v1;
		V2 v2;
		V3 v3;
		kmarshal()->unmarshal(kstr, &k);
		v1marshal()->unmarshal(vstr1, &v1);
		v2marshal()->unmarshal(vstr2, &v2);
		v3marshal()->unmarshal(vstr3, &v3);
		put(k, v1, v2, v3);
	}

protected:
	virtual Marshal<K> *kmarshal() = 0;
	virtual Marshal<V1> *v1marshal() = 0;
	virtual Marshal<V2> *v2marshal() = 0;
	virtual Marshal<V3> *v3marshal() = 0;
};

//template<class K, class V1, class V2, class V3>
//struct TypedTableIterator: public TableIterator{
//	virtual const K& key() = 0;
//	virtual V1& value1() = 0;
//	virtual V2& value2() = 0;
//	virtual V3& value3() = 0;
//
//	virtual void key_str(std::string *out){
//		kmarshal()->marshal(key(), out);
//	}
//	virtual void value1_str(std::string *out){
//		v1marshal()->marshal(value1(), out);
//	}
//	virtual void value2_str(std::string *out){
//		v2marshal()->marshal(value2(), out);
//	}
//	virtual void value3_str(std::string *out){
//		v3marshal()->marshal(value3(), out);
//	}
//
//protected:
//	virtual Marshal<K> *kmarshal(){
//		static Marshal<K> m;
//		return &m;
//	}
//
//	virtual Marshal<V1> *v1marshal(){
//		static Marshal<V1> m;
//		return &m;
//	}
//
//	virtual Marshal<V2> *v2marshal(){
//		static Marshal<V2> m;
//		return &m;
//	}
//
//	virtual Marshal<V3> *v3marshal(){
//		static Marshal<V3> m;
//		return &m;
//	}
//};

//template<class K, class V1, class V2, class V3>
//struct TypedTableIterator;

// Key/value typed interface.
template<class K, class V1, class D>
class PTypedTable: virtual public UntypedTable{
public:
	virtual bool contains(const K &k) = 0;
	virtual V1 get(const K &k) = 0;
	virtual void put(const K &k, const V1 &v1) = 0;
	virtual void update(const K &k, const V1 &v) = 0;
	virtual void accumulate(const K &k, const V1 &v) = 0;
	virtual bool remove(const K &k) = 0;

	// Default specialization for untyped methods
	virtual bool contains_str(const StringPiece& s){
		K k;
		kmarshal()->unmarshal(s, &k);
		return contains(k);
	}

	virtual std::string get_str(const StringPiece &s){
		K k;
		std::string out;

		kmarshal()->unmarshal(s, &k);
		v1marshal()->marshal(get(k), &out);
		return out;
	}

	virtual void update_str(const StringPiece& kstr, const StringPiece &vstr1,
			const StringPiece &vstr2, const StringPiece &vstr3){
		K k;
		V1 v1;
		kmarshal()->unmarshal(kstr, &k);
		v1marshal()->unmarshal(vstr1, &v1);
		put(k, v1);
	}

protected:
	virtual Marshal<K> *kmarshal() = 0;
	virtual Marshal<V1> *v1marshal() = 0;
};

//template<class K, class V1>
//struct PTypedTableIterator: public TableIterator{
//	virtual const K& key() = 0;
//	virtual V1& value1() = 0;
//
//	virtual void key_str(std::string *out){
//		kmarshal()->marshal(key(), out);
//	}
//	virtual void value1_str(std::string *out){
//		v1marshal()->marshal(value1(), out);
//	}
//	virtual void value2_str(std::string *out){
//		v1marshal()->marshal(value1(), out);
//	}
//	virtual void value3_str(std::string *out){
//		v1marshal()->marshal(value1(), out);
//	}
//
//protected:
//	virtual Marshal<K> *kmarshal(){
//		static Marshal<K> m;
//		return &m;
//	}
//
//	virtual Marshal<V1> *v1marshal(){
//		static Marshal<V1> m;
//		return &m;
//	}
//};

struct DecodeIteratorBase {};


// Added for the sake of triggering on remote updates/puts <CRM>
template <typename K, typename V1, typename V2, typename V3>
struct FileDecodeIterator :
		public TypedTableIterator<K, V1, V2, V3>, public DecodeIteratorBase
{
	Marshal<K>* kmarshal(){
		return nullptr;
	}
	Marshal<V1>* v1marshal(){
		return nullptr;
	}
	Marshal<V2>* v2marshal(){
		return nullptr;
	}
	Marshal<V3>* v3marshal(){
		return nullptr;
	}

	FileDecodeIterator(){
		clear();
		rewind();
	}
	void append(K k, V1 v1, V2 v2, V3 v3){
		ClutterRecord<K, V1, V2, V3> thispair(k, v1, v2, v3);
		decodedeque.push_back(thispair);
//    LOG(ERROR) << "APPEND";
	}
	void clear(){
		decodedeque.clear();
//    LOG(ERROR) << "CLEAR";
	}
	void rewind(){
		intit = decodedeque.begin();
//    LOG(ERROR) << "REWIND: empty? " << (intit == decodedeque.end());
	}
	bool done(){
		return intit == decodedeque.end();
	}
	bool Next(){
		intit++;
		return true;
	}
	const K& key(){
		static K k2;
		if(intit != decodedeque.end()){
			k2 = intit->k;
		}
		return k2;
	}
	V1& value1(){
		static V1 vv;
		if(intit != decodedeque.end()){
			vv = intit->v1;
		}
		return vv;
	}
	V2& value2(){
		static V2 vv;
		if(intit != decodedeque.end()){
			vv = intit->v2;
		}
		return vv;
	}
	V3& value3(){
		static V3 vv;
		if(intit != decodedeque.end()){
			vv = intit->v3;
		}
		return vv;
	}

private:
	std::vector<ClutterRecord<K, V1, V2, V3> > decodedeque;
	typename std::vector<ClutterRecord<K, V1, V2, V3> >::iterator intit;
};

template <typename K, typename V1>
struct NetDecodeIterator :
		public PTypedTableIterator<K, V1>, public DecodeIteratorBase
{
	Marshal<K>* kmarshal(){
		return nullptr;
	}
	Marshal<V1>* v1marshal(){
		return nullptr;
	}

	NetDecodeIterator(){
		clear();
		rewind();
	}
	void append(K k, V1 v1){
		std::pair<K, V1> thispair(k, v1);
		decodedeque.push_back(thispair);
//    LOG(ERROR) << "APPEND";
	}
	void clear(){
		decodedeque.clear();
//    LOG(ERROR) << "CLEAR";
	}
	void rewind(){
		intit = decodedeque.begin();
//    LOG(ERROR) << "REWIND: empty? " << (intit == decodedeque.end());
	}
	bool done(){
		return intit == decodedeque.end();
	}
	bool Next(){
		intit++;
		return true;
	}
	const K& key(){
		static K k2;
		if(intit != decodedeque.end()){
			k2 = intit->first;
		}
		return k2;
	}
	V1& value1(){
		static V1 vv;
		if(intit != decodedeque.end()){
			vv = intit->second;
		}
		return vv;
	}

private:
	std::vector<pair<K, V1> > decodedeque;
	typename std::vector<pair<K, V1> >::iterator intit;
};

// Checkpoint and restoration.
class Checkpointable{
public:
	virtual void start_checkpoint(const std::string& f) = 0;
	virtual void write_delta(const KVPairData& put) = 0;
	virtual void finish_checkpoint() = 0;
	virtual void restore(const std::string& f) = 0;
	virtual ~Checkpointable(){}
};

// Interface for serializing tables, either to disk or for transmitting via RPC.
struct TableCoder{
	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3) = 0;
	virtual bool ReadEntryFromFile(std::string* k, std::string *v1, std::string *v2, std::string *v3) = 0;

	virtual ~TableCoder(){}
};

// Interface for serializing tables, either to disk or for transmitting via RPC.
struct KVPairCoder{
	virtual void WriteEntryToNet(StringPiece k, StringPiece v1) = 0;
	virtual bool ReadEntryFromNet(std::string* k, std::string *v1) = 0;

	virtual ~KVPairCoder(){}
};

class Serializable{
public:
	virtual void deserializeFromFile(TableCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToFile(TableCoder* out) = 0;
	virtual ~Serializable(){}
};

class Transmittable{
public:
	virtual void deserializeFromNet(KVPairCoder *in, DecodeIteratorBase *it) = 0;
	virtual void serializeToNet(KVPairCoder* out) = 0;
	virtual ~Transmittable(){}
};

class Snapshottable{
public:
	virtual void serializeToSnapshot(const std::string& f, long* updates, double* totalF2) = 0;
	virtual ~Snapshottable(){}
};

}

#endif
