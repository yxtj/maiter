#include "util/common.h"
//#include "util/file.h"
#include "util/static-initializers.h"

//#include <execinfo.h>

#include <cmath>
#include <ios>

#include <ctime>

//#include <lzo/lzo1x.h>

#ifdef CPUPROF
#include <google/profiler.h>
DEFINE_bool(cpu_profile, false, "");
#endif

#ifdef HEAPPROF
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#endif

using namespace std;

namespace dsm {

const double Histogram::kMinVal = 1e-9;
const double Histogram::kLogBase = 1.1;

int Histogram::bucketForVal(double v){
	if(v < kMinVal){
		return 0;
	}

	v /= kMinVal;
	v += kLogBase;

	return 1 + static_cast<int>(log(v) / log(kLogBase));
}

double Histogram::valForBucket(int b){
	if(b == 0){
		return 0;
	}
	return exp(log(kLogBase) * (b - 1)) * kMinVal;
}

void Histogram::add(double val){
	int b = bucketForVal(val);
//  LOG_EVERY_N(INFO, 1000) << "Adding... " << val << " : " << b;
	if(buckets.size() <= b){
		buckets.resize(b + 1);
	}
	++buckets[b];
	++count;
}

void DumpProfile(){
#ifdef CPUPROF
	ProfilerFlush();
#endif
}

string Histogram::summary(){
	string out;
	int total = 0;
	for(int i = 0; i < buckets.size(); ++i){
		total += buckets[i];
	}
	string hashes = string(100, '#');

	for(int i = 0; i < buckets.size(); ++i){
		if(buckets[i] == 0){
			continue;
		}
		out += StringPrintf("%-20.3g %6d %.*s\n", valForBucket(i), buckets[i],
				buckets[i] * 80 / total, hashes.c_str());
	}
	return out;
}

uint64_t get_memory_total(){
	uint64_t m = -1;
	FILE* procinfo = fopen(StringPrintf("/proc/meminfo", getpid()).c_str(), "r");
	while(fscanf(procinfo, "MemTotal: %ld kB", &m) != 1){
		if(fgetc(procinfo) == EOF){
			break;
		}
	}
	fclose(procinfo);

	return m * 1024;
}

uint64_t get_memory_rss(){
	uint64_t m = -1;
	FILE* procinfo = fopen(StringPrintf("/proc/%d/status", getpid()).c_str(), "r");
	while(fscanf(procinfo, "VmRSS: %ld kB", &m) != 1){
		if(fgetc(procinfo) == EOF){
			break;
		}
	}
	fclose(procinfo);

	return m * 1024;
}

//static void FatalSignalHandler(int sig){
//	fprintf(stderr, "Fatal error; signal %d occurred.\n", sig);
//	static SpinLock lock;
//	static void* stack[128];
//
//	lock.lock();
//
//	if(!FLAGS_dump_stacktrace){
//		_exit(1);
//	}
//
//	int count = backtrace(stack, 128);
//	backtrace_symbols_fd(stack, count, STDERR_FILENO);
//
//	static char cmdbuffer[1024];
//	snprintf(cmdbuffer, 1024,
//			"gdb "
//					"-p %d "
//					"-ex 'set print pretty' "
//					"-ex 'set pagination 0' "
//					"-ex 'thread apply all bt ' "
//					"-batch ", getpid());
//
//	fprintf(stderr, "Calling gdb with: %s", cmdbuffer);
//
//	system(cmdbuffer);
//	_exit(1);
//}

} //namespace dsm
