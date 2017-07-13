/*
 * termchecker.h
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_TERM_CHECKER_H_
#define KERNEL_TERM_CHECKER_H_

#include <vector>
#include <limits>
#include <utility>
#include "table/table_iterator.h"

namespace dsm{

struct TermCheckerBase {
	std::pair<double, int64_t> last;
	std::pair<double, int64_t> curr;
	TermCheckerBase():last(-std::numeric_limits<double>::max(), 0), curr(0.0, 0){}
	std::pair<double, int64_t> get_curr() const { return curr; }
};

template<class K, class V>
struct TermChecker: public TermCheckerBase{
	//generate local report, the default version is summing over all entries
	virtual double estimate_prog(LocalTableIterator<K, V>* table_itr);
	//decide whether to terminate with
	virtual bool terminate(const std::vector<std::pair<double, uint64_t>>& local_reports)=0;
	virtual ~TermChecker(){}
};

template<class K, class V>
double TermChecker<K,V>::estimate_prog(LocalTableIterator<K, V>* statetable){
	double partial_curr = 0;
	V defaultv = statetable->defaultV();
	while(!statetable->done()){
		//cout << statetable->key() << "\t" << statetable->value2() << endl;
		if(statetable->value2() != defaultv){
			partial_curr += static_cast<double>(statetable->value2());
		}
		statetable->Next();
	}
	return partial_curr;
}

} //namespace std


#endif /* KERNEL_TERM_CHECKER_H_ */
