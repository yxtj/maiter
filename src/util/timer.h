#ifndef TIMER_H_
#define TIMER_H_

#include <cstdint>
#include <ratio>

namespace dsm {

uint64_t NowNanoseconds();

double Now();

//double get_processor_frequency();

class Timer {
public:
  Timer() {
    Reset();
  }

  void Reset() {
    start_time_ = Now();
//    start_cycle_ = rdtsc();
  }

  double elapsed() const {
    return Now() - start_time_;
  }

//  uint64_t cycles_elapsed() const {
//    return rdtsc() - start_cycle_;
//  }
//
//  // Rate at which an event occurs.
//  double rate(int count) {
//    return count / (Now() - start_time_);
//  }
//
//  double cycle_rate(int count) {
//    return double(cycles_elapsed()) / count;
//  }

private:
  double start_time_;
//  uint64_t start_cycle_;
};

}

#define EVERY_N(interval, operation)\
{ static unsigned COUNT = 0;\
  if (COUNT++ % interval == 0) {\
    operation;\
  }\
}

/*
#define PERIODIC(interval, operation)\
{ static uint64_t last = 0;\
  static uint64_t cycles = (int64_t)(interval * get_processor_frequency());\
  uint64_t now = rdtsc(); \
  if (now - last > cycles) {\
    last = now;\
    operation;\
  }\
}
*/

#define PERIODIC(interval, operation)\
{ static uint64_t last = 0;\
  static uint64_t cycles = (int64_t)(interval * std::giga::num);\
  uint64_t now = NowNanoseconds(); \
  if (now - last > cycles) {\
    last = now;\
    operation;\
  }\
}

#endif /* TIMER_H_ */
