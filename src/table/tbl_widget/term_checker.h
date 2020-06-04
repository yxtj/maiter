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
#include "table/table_iterator.h"

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
	virtual double single_progress(const V& v2);
	//void update_single(LocalTableIterator<K, V>* iter);
	//decide whether to terminate with
	virtual bool terminate(const std::vector<double>& local_reports)=0;
	virtual ~TermChecker(){}
};

template<class K, class V>
inline double TermChecker<K,V>::estimate_prog(LocalTableIterator<K, V>* statetable){
	double partial_curr = 0;
	V defaultv = statetable->defaultV();
	while(!statetable->done()){
		//cout << statetable->key() << "\t" << statetable->value2() << endl;
		if(statetable->value2() != defaultv){
			partial_curr += single_progress(statetable->value2());
		}
		statetable->Next();
	}
	return partial_curr;
}

template<class K, class V>
inline double TermChecker<K, V>::single_progress(const V& v2)
{
	return static_cast<double>(v2);
}

#endif //SWIG

} //namespace std


#endif /* KERNEL_TERM_CHECKER_H_ */
