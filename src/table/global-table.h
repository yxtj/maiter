#ifndef GLOBAL_TABLE_H_
#define GLOBAL_TABLE_H_

#include "local-table.h"
#include "table.h"
#include "table-interfaces.h"
#include "util/timer.h"
#include <mutex>
#include <unordered_map>

//#define GLOBAL_TABLE_USE_SCOPEDLOCK

namespace dsm {

class Worker;
class Master;

// Encodes table entries using the passed in TableData protocol buffer.
struct ProtoTableCoder: public TableCoder{
	ProtoTableCoder(const TableData* in);
	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
	virtual bool ReadEntryFromFile(string* k, string *v1, string *v2, string *v3);

	int read_pos_;
	TableData *t_;
};

// Encodes table entries using the passed in TableData protocol buffer.
struct ProtoKVPairCoder: public KVPairCoder{
	ProtoKVPairCoder(const KVPairData* in);
	virtual void WriteEntryToNet(StringPiece k, StringPiece v1);
	virtual bool ReadEntryFromNet(string* k, string *v1);

	int read_pos_;
	KVPairData *t_;
};

struct PartitionInfo{
	PartitionInfo() :
			dirty(false), tainted(false){
	}
	bool dirty;
	bool tainted;
	ShardInfo sinfo;
};

class GlobalTableBase: virtual public Table{
public:
	virtual void UpdatePartitions(const ShardInfo& sinfo) = 0;
	virtual TableIterator* get_iterator(int shard, bool bfilter,
			unsigned int fetch_num = FETCH_NUM) = 0;

	virtual bool is_local_shard(int shard) = 0;
	virtual bool is_local_key(const StringPiece &k) = 0;

	virtual PartitionInfo* get_partition_info(int shard) = 0;
	virtual LocalTable* get_partition(int shard) = 0;

	virtual bool tainted(int shard) = 0;
	virtual int owner(int shard) = 0;

protected:
	friend class Worker;
	friend class Master;

	virtual int64_t shard_size(int shard) = 0;
};

class MutableGlobalTableBase: virtual public GlobalTableBase{
public:
	MutableGlobalTableBase();

	// Four main working steps: merge - process - send - term
	virtual void MergeUpdates(const KVPairData& req) = 0;
	virtual void ProcessUpdates() = 0;
	virtual void SendUpdates() = 0;
	virtual void TermCheck() = 0;

	//helpers for main working loop (for conditional invocation)
	void BufProcessUpdates();
	void BufSendUpdates();
	void BufTermCheck();

	// XXX: evolving graph
	virtual void add_ineighbor(const InNeighborData& req) = 0;
	virtual void ProcessRequest(const ValueRequest& req) = 0;

	bool allowProcess() const { return allow2Process_; }
	void enableProcess(){ allow2Process_=true; }
	void disableProcess(){ allow2Process_=false; }
	bool is_processing() const { return processing_; }
	bool is_sending() const { return sending_; }
	bool is_termchecking() const { return termchecking_; }

	//helpers for main working loop
	void resetProcessMarker();
	void resetSendMarker();
	void resetTermMarker();
	//helpers for main working loop (for checking availability)
	bool canProcess();
	bool canSend();
	bool canPnS();
	bool canTermCheck();

	virtual int64_t pending_write_bytes() = 0;

	virtual bool initialized() = 0;
	virtual void resize(int64_t new_size) = 0;

	virtual void clear() = 0;
	// Exchange the content of this table with that of table 'b'.
	virtual void swap(GlobalTableBase *b) = 0;
	virtual void local_swap(GlobalTableBase *b) = 0;

protected:
	bool allow2Process_ = true;
	bool processing_ = false;
	bool sending_ = false;
	bool termchecking_ = false;

	Timer tmr_process, tmr_send, tmr_term;
	int64_t pending_process_;
	int64_t pending_send_;

	int64_t bufmsg;	//updated at InitStateTable with (state-table-size * bufmsg_portion)
	double buftime; //initialized in the constructor with min(FLAGS_buftime, FLAGS_snapshot_interval/4)
};

class GlobalTable: virtual public GlobalTableBase{
public:
	virtual ~GlobalTable();

	void Init(const TableDescriptor *tinfo);

	void UpdatePartitions(const ShardInfo& sinfo);

	virtual TableIterator* get_iterator(int shard, bool bfilter,
			unsigned int fetch_num = FETCH_NUM) = 0;

	virtual bool is_local_shard(int shard);
	virtual bool is_local_key(const StringPiece &k);

	int64_t shard_size(int shard);

	PartitionInfo* get_partition_info(int shard){
		return &partinfo_[shard];
	}
	LocalTable* get_partition(int shard){
		return partitions_[shard];
	}

	bool tainted(int shard){
		return get_partition_info(shard)->tainted;
	}
	int owner(int shard){
		return get_partition_info(shard)->sinfo.owner();
	}
protected:
	virtual int shard_for_key_str(const StringPiece& k) = 0;

	std::vector<LocalTable*> partitions_;
	std::vector<LocalTable*> cache_;

	std::recursive_mutex m_;
	std::mutex m_trig_;
	std::recursive_mutex& get_mutex(){
		return m_;
	}
	std::mutex& trigger_mutex(){
		return m_trig_;
	}

	std::vector<PartitionInfo> partinfo_;

	struct CacheEntry{
		double last_read_time;
		string value;
	};

	std::unordered_map<StringPiece, CacheEntry> remote_cache_;
};

class MutableGlobalTable:
		virtual public GlobalTable,
		virtual public MutableGlobalTableBase,
		virtual public Checkpointable{
public:
	MutableGlobalTable();

	//main working loop
	virtual void MergeUpdates(const KVPairData& req) = 0;
	virtual void ProcessUpdates() = 0;
	virtual void SendUpdates();
	virtual void TermCheck();

	void InitUpdateBuffer();
	int64_t pending_write_bytes();

	void clear();
	void resize(int64_t new_size);

	//override from Checkpointable
	void start_checkpoint(const string& pre);
	void write_message(const KVPairData& d);
	void finish_checkpoint();
	void restore(const string& pre);
	//convenient functions for checkpoint
	void start_checkpoint(const int taskid, const int epoch);
	void restore(const int taskid, const int epoch);

	void swap(GlobalTableBase *b);
	void local_swap(GlobalTableBase *b);

//	int64_t sent_bytes_;

protected:
	void setUpdatesFromAggregated();	// aggregated way
	void addIntoUpdateBuffer(int shard, Arg& arg);	// non-aggregated way

	std::vector<KVPairData> update_buffer;
	std::mutex m_buff;

	int snapshot_index;
};

}

#endif /* GLOBAL_TABLE_H_ */
