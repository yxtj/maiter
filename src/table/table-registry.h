#ifndef TABLE_REGISTY_H_
#define TABLE_REGISTY_H_

#include "table/TableDescriptor.h"
#include "global-table.h"
#include "util/noncopyable.h"

#include <map>

static const int kStatsTableId = 1000000;

namespace dsm {

class GlobalTableBase;
class MutableGlobalTableBase;

class TableRegistry: private noncopyable{
public:
	typedef std::map<int, GlobalTableBase*> Map;

	static TableRegistry* Get(); //singleton

	Map& tables();
	GlobalTableBase* table(int id);
	MutableGlobalTableBase* mutable_table(int id);

private:
	Map tmap_;
	TableRegistry(){}
};

} // end namespace
#endif /* TABLE_REGISTY_H_ */
