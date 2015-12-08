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
#include "table_iterator.h"

namespace dsm{

struct TermCheckerBase {
	double last;
	double curr;
	TermCheckerBase():last(-std::numeric_limits<double>::max()), curr(0){}
	double get_curr() { return curr; }
};

#ifndef SWIG

template<class K, class V>
struct TermChecker: public TermCheckerBase{
	//generate local report, the default version is summing over all entries
	virtual double estimate_prog(LocalTableIterator<K, V>* table_itr);
	//decide whether to terminate with
	virtual bool terminate(const std::vector<double>& local_reports)=0;
	virtual ~TermChecker(){}
};

template<class K, class V>
double TermChecker<K,V>::estimate_prog(LocalTableIterator<K, V>* statetable){
	double partial_curr = 0;
	V defaultv = statetable->defaultV();
	while(!statetable->done()){
		bool cont = statetable->Next();
		if(!cont) break;
		//statetable->Next();
		//cout << statetable->key() << "\t" << statetable->value2() << endl;
		if(statetable->value2() != defaultv){
			partial_curr += static_cast<double>(statetable->value2());
		}
	}
	return partial_curr;
}

#endif //SWIG

} //namespace std


#endif /* KERNEL_TERM_CHECKER_H_ */
