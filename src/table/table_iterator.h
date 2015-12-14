/*
 * tableiterator.h
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_TABLE_ITERATOR_H_
#define KERNEL_TABLE_ITERATOR_H_

#include <string>
#include "util/marshal.hpp"

namespace dsm{

struct TableIterator {
  virtual void key_str(std::string *out) = 0;
  virtual void value1_str(std::string *out) = 0;
  virtual void value2_str(std::string *out) = 0;
  virtual void value3_str(std::string *out) = 0;
  virtual bool done() = 0;
  virtual bool Next() = 0;
  virtual ~TableIterator(){}
};

template <class K, class V>
struct LocalTableIterator {
    virtual const K& key() = 0;
    virtual V& value2() = 0;
    virtual bool done() = 0;
    virtual bool Next() = 0;
    virtual V defaultV() = 0;
    virtual ~LocalTableIterator(){}
};


template <class K, class V1, class V2, class V3>
struct TypedTableIterator : public TableIterator {
	virtual const K& key() = 0;
	virtual V1& value1() = 0;
	virtual V2& value2() = 0;
	virtual V3& value3() = 0;

	virtual void key_str(std::string *out) { kmarshal()->marshal(key(), out); }
	virtual void value1_str(std::string *out) { v1marshal()->marshal(value1(), out); }
	virtual void value2_str(std::string *out) { v2marshal()->marshal(value2(), out); }
	virtual void value3_str(std::string *out) { v3marshal()->marshal(value3(), out); }

protected:
	virtual Marshal<K> *kmarshal(){
		static Marshal<K> m;
		return &m;
	}
	virtual Marshal<V1> *v1marshal(){
		static Marshal<V1> m;
		return &m;
	}
	virtual Marshal<V2> *v2marshal(){
		static Marshal<V2> m;
		return &m;
	}
	virtual Marshal<V3> *v3marshal(){
		static Marshal<V3> m;
		return &m;
	}
};

template <class K, class V1>
struct PTypedTableIterator : public TableIterator {
	virtual const K& key() = 0;
	virtual V1& value1() = 0;

	virtual void key_str(std::string *out) { kmarshal()->marshal(key(), out); }
	virtual void value1_str(std::string *out) { v1marshal()->marshal(value1(), out); }
	//value2_str and value3_str are there for TableIterator
	virtual void value2_str(std::string *out) {}
	virtual void value3_str(std::string *out) {}

protected:
	virtual Marshal<K> *kmarshal(){
		static Marshal<K> m;
		return &m;
	}

	virtual Marshal<V1> *v1marshal(){
		static Marshal<V1> m;
		return &m;
	}
};


} //namespace dsm

#endif /* KERNEL_TABLE_ITERATOR_H_ */
