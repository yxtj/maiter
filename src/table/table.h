#ifndef TABLE_TABLE_H_
#define TABLE_TABLE_H_

#include "TableDescriptor.h"
#include "msg/message.pb.h"

#include "TableHelper.h"
#include "table_iterator.h"
#include "tbl_widget/trigger.h"
//#include "tbl_widget/sharder.h"
//#include "tbl_widget/term_checker.h"

#include <glog/logging.h>

#include <string>
#include <vector>
#include <utility>
#include <ostream>


static constexpr int FETCH_NUM = (2048);

namespace dsm {

struct Table;

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

	std::ostream& operator<<(std::ostream& out){
		return out << k << "\t" << v1 << "|" << v2 << "|" << v3;
	}
};


struct TableFactory{
	virtual Table* New() = 0;
	virtual ~TableFactory(){}
};

struct TableBase{
	virtual const TableDescriptor& info() const = 0;
	virtual TableDescriptor& mutable_info() = 0;
	virtual int id() const = 0;
	virtual int num_shards() const = 0;
	virtual ~TableBase(){}
};

struct UntypedTable{
	virtual bool contains_str(const StringPiece& k) = 0;
	virtual std::string get_str(const StringPiece &k) = 0;
	virtual void update_str(const StringPiece &k, const StringPiece &v1, const StringPiece &v2,
			const StringPiece &v3) = 0;
	virtual ~UntypedTable(){}
};

// Methods common to both global and local table views.
class Table: public TableBase{
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
	bool alive() const{
		return !terminated_;
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
	virtual void put(K &&k, V1 &&v1, V2 &&v2, V3 &&v3) = 0;
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

}

#endif
