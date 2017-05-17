/*
 * termchecker.hpp
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_TERMCHECKER_HPP_
#define KERNEL_TERMCHECKER_HPP_

#include "term_checker.h"

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
				if(statetable->value2() != defaultv){
					partial_curr += static_cast<double>(statetable->value2());
				}
				statetable->Next();
			}
			return partial_curr;
		}
		virtual bool terminate(const std::vector<std::pair<double, uint64_t> >& local_reports){
			//your aggregation logic for using local reports here (e.g summation)
			curr = std::make_pair<double, int64_t>(0.0, 0);
			for(auto& p : local_reports){
				curr.first += p.first;
				curr.second += p.second;
			}
			VLOG(0) << "terminate check : last progress (" << last.first <<" , "<<last.second
					<< ") current progress (" << curr.first<<" , "<<curr.second
					<< ") difference (" << (curr.first - last.first) <<" , "
					<< (curr.second - last.second)<<")";
			//your termination condition here (e.g. difference to last)
			if(curr.second == last.second &&
					std::abs(curr.first - last.first) <= FLAGS_termcheck_threshold){
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
		bool terminate(const std::vector<std::pair<double, uint64_t>>& local_reports){
			curr = std::make_pair<double, int64_t>(0.0, 0);
			for(auto& p : local_reports){
				curr.first += p.first;
				curr.second += p.second;
			}
			VLOG(0) << "terminate check : last progress (" << last.first <<" , "<<last.second
					<< ") current progress (" << curr.first<<" , "<<curr.second
					<< ") difference (" << (curr.first - last.first) <<" , "
					<< (curr.second - last.second)<<")";
			if(curr.second == last.second &&
					std::abs(curr.first - last.first) <= FLAGS_termcheck_threshold){
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
		bool terminate(const std::vector<std::pair<double, uint64_t>>& local_reports){
			curr = std::make_pair<double, int64_t>(0.0, 0);
			for(auto& p : local_reports){
				curr.first += p.first;
				curr.second += p.second;
			}
			VLOG(0) << "terminate check : last progress (" << last.first <<" , "<<last.second
					<< ") current progress (" << curr.first<<" , "<<curr.second
					<< ") difference (" << (curr.first - last.first) <<" , "
					<< (curr.second - last.second)<<")";
			if(std::abs(curr.first) >= FLAGS_termcheck_threshold){
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
