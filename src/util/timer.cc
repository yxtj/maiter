/*
 * timer.cc
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#include "timer.h"

namespace dsm{


using namespace std;

std::chrono::system_clock::time_point Timer::_boot_time = chrono::system_clock::now();

Timer::Timer()
{
	Reset();
}

void Timer::Reset()
{
	_time = chrono::system_clock::now();
}

long long Timer::elapseMS() const
{
	return chrono::duration_cast<chrono::milliseconds>(
		chrono::system_clock::now() - _time).count();
}

int Timer::elapseS() const
{
	return chrono::duration_cast<chrono::duration<int>>(
		chrono::system_clock::now() - _time).count();
}

double Timer::elapseSd() const
{
	return chrono::duration_cast<chrono::duration<double>>(
		chrono::system_clock::now() - _time).count();
}

double Timer::elapseMin() const
{
	chrono::duration<double, ratio<60> > passed = chrono::system_clock::now() - _time;
	return passed.count();
}

double Timer::Now(){
	return chrono::duration<double>(
		chrono::system_clock::now().time_since_epoch()).count();
}

double Timer::NowSinceBoot(){
	return chrono::duration_cast<chrono::duration<double>>(
		chrono::system_clock::now() - _boot_time).count();
}

}
