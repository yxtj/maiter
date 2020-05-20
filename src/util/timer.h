#ifndef TIMER_H_
#define TIMER_H_

//#include <time.h>
//#include <stdint.h>
#include <chrono>

namespace dsm {

class Timer
{
	std::chrono::system_clock::time_point _time;
	static std::chrono::system_clock::time_point _boot_time;
public:
	Timer();
	void Reset();

	double elapsed() const {
		return elapseSd();
	}
	long long elapseMS() const;
	int elapseS() const;
	double elapseSd() const;
	double elapseMin() const;

	static double Now();
	static double NowSinceBoot();
};

double Now() { return Timer::Now(); }

#define EVERY_N(interval, operation)\
{ static int COUNT = 0;\
  if (COUNT++ % interval == 0) {\
    operation;\
  }\
}

#define PERIODIC(interval, operation)\
{ static double last = 0;\
  double now = Timer::Now(); \
  if (now - last > interval) {\
    last = now;\
    operation;\
  }\
}

}

#endif /* TIMER_H_ */
