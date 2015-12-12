#ifndef TABLE_REGISTY_H_
#define TABLE_REGISTY_H_

#include "global-table.h"
#include "typed-global-table.hpp"
#include "statetable.h"
#include "deltatable.h"
#include "table_descriptor.h"

//#include "tbl_widget/sharder.h"
//#include "tbl_widget/term_checker.h"
//#include "tbl_widget/IterateKernel.h"
#include "util/marshal.hpp"
#include "util/noncopyable.h"

#include <map>

static const int kStatsTableId = 1000000;

namespace dsm {


template <class K>
class Sharder;
template <class K, class V1, class V3>
class IterateKernel;
template <class K, class V2>
class TermChecker;

class TableRegistry : private noncopyable {
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
#endif /* TABLE_REGISTY_H_ */
