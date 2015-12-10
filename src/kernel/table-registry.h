#ifndef KERNEL_H_
#define KERNEL_H_

//#include "kernel/local-table.h"
//#include "util/common.h"
//#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/typed-global-table.hpp"
#include "kernel/statetable.h"
#include "kernel/deltatable.h"
#include "kernel/table_descriptor.h"

#include "kernel/sharder.h"
#include "kernel/term_checker.h"
#include "kernel/kernel/IterateKernel.h"
#include "util/marshal.hpp"

static const int kStatsTableId = 1000000;

namespace dsm {

class TableRegistry : private boost::noncopyable {
private:
  TableRegistry() {}
public:
  typedef std::map<int, GlobalTableBase*> Map;

  static TableRegistry* Get();

  Map& tables();
  GlobalTableBase* table(int id);
  MutableGlobalTableBase* mutable_table(int id);

private:
  Map tmap_;
};

// Swig doesn't like templatized default arguments; work around that here.
template<class K, class V1, class V2, class V3>
static TypedGlobalTable<K, V1, V2, V3>* CreateTable(int id, int shards, double schedule_portion,
                                           Sharder<K>* sharding,
                                           IterateKernel<K, V1, V3>* iterkernel,
                                           TermChecker<K, V2>* termchecker) {
  TableDescriptor *info = new TableDescriptor(id, shards);
  info->key_marshal = new Marshal<K>;
  info->value1_marshal = new Marshal<V1>;
  info->value2_marshal = new Marshal<V2>;
  info->value3_marshal = new Marshal<V3>;
  info->sharder = sharding;
  info->iterkernel = iterkernel;
  info->termchecker = termchecker;
  info->partition_factory = new typename StateTable<K, V1, V2, V3>::Factory;
  info->deltaT_factory = new typename DeltaTable<K, V1, V3>::Factory;
  info->schedule_portion = schedule_portion;

  return CreateTable<K, V1, V2, V3>(info);
}

template<class K, class V1, class V2, class V3>
static TypedGlobalTable<K, V1, V2, V3>* CreateTable(const TableDescriptor *info) {
  TypedGlobalTable<K, V1, V2, V3> *t = new TypedGlobalTable<K, V1, V2, V3>();
  t->Init(info);
  TableRegistry::Get()->tables().insert(make_pair(info->table_id, t));
  return t;
}

} // end namespace
#endif /* KERNEL_H_ */
