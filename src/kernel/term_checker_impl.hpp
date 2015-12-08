/*
 * termchecker.hpp
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_TERMCHECKER_HPP_
#define KERNEL_TERMCHECKER_HPP_

#include "term_checker.h"

#include <vector>
#include <algorithm>

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_double(termcheck_threshold);

namespace dsm{

template<class K, class V>
struct TermCheckers{
	struct CompleteExample : public TermChecker<K, V> {
		using TermChecker<K,V>::last;
		using TermChecker<K,V>::curr;
		virtual double estimate_prog(LocalTableIterator<K, V>* statetable){
			//your logic for local report (e.g summation)
			double partial_curr = 0;
			V defaultv = statetable->defaultV();
			while(!statetable->done()){
				bool cont = statetable->Next();
				if(!cont) break;
				if(statetable->value2() != defaultv){
					partial_curr += static_cast<double>(statetable->value2());
				}
			}
			return partial_curr;
		}
		virtual bool terminate(const std::vector<double>& local_reports){
			//your aggregation logic for using local reports here (e.g summation)
			curr = std::accumulate(local_reports.begin(),local_reports.end(),0);
			VLOG(0) << "terminate check : last progress " << last << " current progress " << curr
					<< " difference " << abs(curr - last);
			//your termination condition here (e.g. difference to last)
			if(std::abs(curr - last) <= FLAGS_termcheck_threshold){
				return true;
			}else{
				last = curr;
				return false;
			}
		}
	};
	struct Diff : public TermChecker<K, V> {
		using TermChecker<K,V>::last;
		using TermChecker<K,V>::curr;
		bool terminate(const std::vector<double>& local_reports){
			curr = std::accumulate(local_reports.begin(),local_reports.end(),0);
			VLOG(0) << "terminate check : last progress " << last << " current progress " << curr
					<< " difference " << abs(curr - last);
			if(std::abs(curr - last) <= FLAGS_termcheck_threshold){
				return true;
			}else{
				last = curr;
				return false;
			}
		}
	};
	struct Sum : public TermChecker<K, V> {
		using TermChecker<K,V>::last;
		using TermChecker<K,V>::curr;
		bool terminate(const std::vector<double>& local_reports){
			curr = std::accumulate(local_reports.begin(),local_reports.end(),0);
			VLOG(0) << "terminate check : last progress " << last << " current progress " << curr
					<< " difference " << abs(curr-last);
			if(abs(curr) >= FLAGS_termcheck_threshold){
				return true;
			}else{
				last = curr;
				return false;
			}
		}
	};
};

}	//namespace dsm

#endif /* KERNEL_TERMCHECKER_HPP_ */
