/*
 * timer.cc
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#include <time.h>
#include <cstdio>
#include <chrono>
#include <thread>

namespace dsm{

using namespace std;
using namespace std::chrono;

uint64_t NowNanoseconds() {
//	uint32_t hi, lo;
//	__asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
//	return (((uint64_t)hi)<<32) | ((uint64_t)lo);

	auto now=system_clock::now().time_since_epoch();
	return duration_cast<nanoseconds>(now).count();
}

double Now(){
//	timespec tp;
//	clock_gettime(CLOCK_MONOTONIC, &tp);
//	return tp.tv_sec + 1e-9 * tp.tv_nsec;

	auto now=system_clock::now().time_since_epoch();
	return duration_cast<duration<double>>(now).count();
}

/*
double get_processor_frequency() {
	double freq;
	int a, b;
	FILE* procinfo = fopen("/proc/cpuinfo", "r");
	while (fscanf(procinfo, "cpu MHz : %d.%d", &a, &b) != 2) {
		fgetc(procinfo);
	}
	fclose(procinfo);

	freq = a * 1e6 + b * 1e-4;
	return freq;
}
*/

void Sleep(const double time)
{
	this_thread::sleep_for(duration<double>(time));
}

} // namespace
