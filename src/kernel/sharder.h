/*
 * sharder.h
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_SHARDER_H_
#define KERNEL_SHARDER_H_


namespace dsm {

struct SharderBase{};

#ifndef SWIG

// Each table is associated with a single accumulator.  Accumulators are
// applied whenever an update is supplied for an existing key-value cell.
template<class K>
struct Sharder: public SharderBase{
	virtual int operator()(const K& k, int shards) = 0;
	virtual ~Sharder(){}
};

#endif //SWIG

} //namespace dsm



#endif /* KERNEL_SHARDER_H_ */
