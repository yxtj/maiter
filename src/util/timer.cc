/*
 * timer.cc
 *
 *  Created on: Dec 13, 2015
 *      Author: tzhou
 */

#include "timer.h"
#include "cstdio"

namespace dsm{

double get_processor_frequency() {
  double freq;
  int a, b;
  FILE* procinfo = fopen("/proc/cpuinfo", "r");
  while (fscanf(procinfo, "cpu MHz : %d.%d", &a, &b) != 2) {
    fgetc(procinfo);
  }

  freq = a * 1e6 + b * 1e-4;
  fclose(procinfo);
  return freq;
}

}
