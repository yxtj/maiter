#include <stdio.h>
#include "kernel/table-registry.h"

#include "kernel/kernel.h"

namespace dsm {

KernelRegistry* KernelRegistry::Get() {
  static KernelRegistry* r = nullptr;
  if (!r) { r = new KernelRegistry; }
  return r;
}

RunnerRegistry* RunnerRegistry::Get() {
  static RunnerRegistry* r = nullptr;
  if (!r) { r = new RunnerRegistry; }
  return r;
}


}
