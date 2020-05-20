#ifndef COMMON_H_
#define COMMON_H_

#include <time.h>
#include <vector>
#include <string>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "glog/logging.h"
#include "gflags/gflags.h"
#include <google/protobuf/message.h>

#include "util/hash.h"
#include "util/static-initializers.h"
#include "util/stringpiece.h"
#include "util/timer.h"
#include "util/tuple.h"
#include "util/noncopyable.h"

#include <unordered_map>
#include <unordered_set>

#include "util/marshal.hpp"

using std::map;
using std::vector;
using std::string;
using std::pair;
using std::make_pair;
using std::unordered_map;
using std::unordered_set;

namespace dsm {

uint64_t get_memory_rss();
uint64_t get_memory_total();

void DumpProfile();

// Log-bucketed histogram.
class Histogram{
public:
	Histogram() :
			count(0){
	}

	void add(double val);
	string summary();

	int bucketForVal(double v);
	double valForBucket(int b);

	int getCount(){
		return count;
	}
private:

	int count;
	vector<int> buckets;
	static const double kMinVal;
	static const double kLogBase;
};

//static double rand_double(){
//	return double(random()) / RAND_MAX;
//}

// Simple wrapper around a string->double map.
struct Stats{
	double& operator[](const string& key){
		return p_[key];
	}

	std::string ToString(const std::string& prefix, bool sort=false){
		std::string out;
		if(sort==true){
			std::map<std::string, double> p_;
			for(const auto& t : this->p_)
				p_[t.first]=t.second;
			for(auto i = p_.begin(); i != p_.end(); ++i){
				out += StringPrintf("%s -- %s : %.2f\n", prefix.c_str(), i->first.c_str(), i->second);
			}
		}else{
			for(auto i = p_.begin(); i != p_.end(); ++i){
				out += StringPrintf("%s -- %s : %.2f\n", prefix.c_str(), i->first.c_str(), i->second);
			}
		}
		return out;
	}

	void Merge(Stats &other){
		for(auto i = other.p_.begin(); i != other.p_.end(); ++i){
			p_[i->first] += i->second;
		}
	}
private:
	unordered_map<string, double> p_;
};

static vector<int> range(int from, int to, int step = 1){
	vector<int> out;
	out.reserve((to-from+step-1)/step);
	for(int i = from; i < to; i+=step){
		out.push_back(i);
	}
	return out;
}

inline vector<int> range(int to){
	return range(0, to);
}

} //namespace dsm

#define IN(container, item) (std::find(container.begin(), container.end(), item) != container.end())
#define COMPILE_ASSERT(x) extern int __dummy[(int)x]


#ifndef SWIG
// operator<< overload to allow protocol buffers to be output from the logging methods.
#include <google/protobuf/message.h>
namespace std {
inline ostream & operator<<(ostream &out, const google::protobuf::Message &q){
	return out<<q.ShortDebugString();
}
} //namespace std
#endif

#endif /* COMMON_H_ */
