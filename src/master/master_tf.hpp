/*
 * master_tf.hpp
 *
 *  Created on: Feb 19, 2016
 *      Author: tzhou
 */

#ifndef MASTER_MASTER_TF_HPP_
#define MASTER_MASTER_TF_HPP_

#include "master.h"
#include "kernel/maiter-kernel.h"
#include <glog/logging.h>

namespace dsm{

//maiter program
template<class K, class V, class D>
void Master::run_maiter(MaiterKernel<K, V, D>* maiter){
	if(maiter->sharder == nullptr){
		LOG(FATAL)<<"sharder is not specified in current kernel";
		return;
	}

	run_all("MaiterKernel1", "run", maiter->table, false, false, false);

	run_all("MaiterKernelLoadDeltaGraph", "run", maiter->table, false, false, false);
//	run_all("MaiterKernelDumpInNeighbor", "run", maiter->table, false, false, false);

	if(maiter->iterkernel != nullptr && maiter->termchecker != nullptr){
		run_all("MaiterKernel2", "map", maiter->table, true, true, true);
	}

	run_all("MaiterKernel3", "run", maiter->table, false, false, false);

//	run_all("MaiterKernelDumpInNeighbor", "run", maiter->table, false, false, false);
}

template<class T>
T& Master::get_cp_var(const std::string& key, T defval){
	if(!cp_vars_.contains(key)){
		cp_vars_.put(key, defval);
	}
	return cp_vars_.get<T>(key);
}


} //namespace dsm

#endif /* MASTER_MASTER_TF_HPP_ */
