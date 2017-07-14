/*
 * DSMKernel.h
 *
 *  Created on: Dec 7, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_DSMKERNEL_H_
#define KERNEL_DSMKERNEL_H_

#include "util/marshalled_map.hpp"
//#include "table/table.h"
//#include "table/global-table.h"
#include <string>

namespace dsm {

class GlobalTableBase;
template<class K, class V1, class V2, class V3>
class TypedGlobalTable;
class Worker;

class DSMKernel{
public:
	// Called upon creation of this kernel by a worker.
	virtual void InitKernel(){}
	virtual ~DSMKernel(){}

	// The table and shard being processed.
	int current_shard() const{
		return shard_;
	}
	int current_table() const{
		return table_id_;
	}

	template<class T>
	T& get_arg(const std::string& key) const{
		return args_.get<T>(key);
	}

	template<class T>
	T& get_cp_var(const std::string& key, T defval = T()){
		if(!cp_.contains(key)){
			cp_.put(key, defval);
		}
		return cp_.get<T>(key);
	}

	GlobalTableBase* get_table(int id);

	template<class K, class V1, class V2, class V3>
	TypedGlobalTable<K, V1, V2, V3>* get_table(int id){
		return dynamic_cast<TypedGlobalTable<K, V1, V2, V3>*>(get_table(id));
	}

//	template<class K, class V, class D>
//	void set_maiter(MaiterKernel<K, V, D> maiter){}

private:
	friend class Worker;
	friend class Master;

	void initialize_internal(Worker* w, int table_id, int shard);

	void set_args(const MarshalledMap& args);
//	void set_checkpoint(const MarshalledMap& args);

	Worker *w_;
	int shard_;
	int table_id_;
	MarshalledMap args_;
	MarshalledMap cp_;
};

} /* namespace dsm */

#endif /* KERNEL_DSMKERNEL_H_ */
