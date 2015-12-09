#include <local-table.h>
#include <stdio.h>
#include <table-registry.h>
#include "kernel/global-table.h"

namespace dsm {

TableRegistry* TableRegistry::Get() {
  static TableRegistry* t = new TableRegistry;
  return t;
}

TableRegistry::Map& TableRegistry::tables() {
  return tmap_;
}

GlobalTableBase* TableRegistry::table(int id) {
  CHECK(tmap_.find(id) != tmap_.end());
  return tmap_[id];
}

MutableGlobalTableBase* TableRegistry::mutable_table(int id) {
  CHECK(tmap_.find(id) != tmap_.end());
  return dynamic_cast<MutableGlobalTable*>(tmap_[id]);
}

}
