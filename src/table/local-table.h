#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"
#include "table-interfaces.h"
#include <string>

namespace dsm {

static const double kLoadFactor = 0.8;

class TableHelper;

// Represents a single shard of a partitioned global table.
class LocalTable: public Table,
		virtual public UntypedTable,
		public Checkpointable,
		public Serializable,
		public Transmittable,
		public Snapshottable{
public:
	LocalTable() :
			delta_file_(nullptr){
	}
	bool empty(){
		return size() == 0;
	}

	void start_checkpoint(const std::string& f);
	void write_message(const KVPairData& put);
	void finish_checkpoint();
	void load_checkpoint(const std::string& f);

	int64_t dump(const std::string& f, TableCoder* out = nullptr);
	void restore(const std::string& f, TableCoder* in = nullptr);
	virtual void restoreState(const std::string& k, const std::string& v1, const std::string& v2) = 0;

	void termcheck(const std::string& f, int64_t*updates, double *totalF2);

	virtual int64_t size() = 0;
	virtual int64_t capacity() = 0;
	virtual void clear() = 0;
	virtual void reset() = 0;
	virtual void resize(int64_t size) = 0;

	virtual TableIterator* get_iterator(TableHelper* helper, bool bfilter) = 0;
	virtual TableIterator* schedule_iterator(TableHelper* helper, bool bfilter) = 0;
	virtual TableIterator* entirepass_iterator(TableHelper* helper) = 0;

	int shard(){
		return info_.shard;
	}

protected:
	friend class GlobalTableBase;
	TableCoder *delta_file_;
};

}

#endif /* LOCALTABLE_H_ */
