#include "local-table.h"
#include "table.h"
#include "util/timer.h"
#include "util/file.h"

#ifndef NDEBUG
#include "dbg/getcallstack.h"
#endif

using namespace std;

namespace dsm {

// Encodes or decodes table entries, reading and writing from the
// specified file.
struct LocalTableCoder : public TableCoder{
	LocalTableCoder(const string& f, const string& mode);
	virtual ~LocalTableCoder();

	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
	virtual bool ReadEntryFromFile(string* k, string* v1, string* v2, string* v3);

	RecordFile* f_;
};

LocalTableCoder::LocalTableCoder(const string& f, const string& mode) :
	f_(new RecordFile(f, mode, RecordFile::NONE))
{
}

LocalTableCoder::~LocalTableCoder(){
	delete f_;
}

bool LocalTableCoder::ReadEntryFromFile(string* k, string* v1, string* v2, string* v3){
	if(f_->readChunk(k)){
		f_->readChunk(v1);
		f_->readChunk(v2);
		f_->readChunk(v3);
		return true;
	}

	return false;
}

void LocalTableCoder::WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2,
	StringPiece v3){
	f_->writeChunk(k);
	f_->writeChunk(v1);
	f_->writeChunk(v2);
	f_->writeChunk(v3);
}


/*
 * Local Table:
 */
//snapshot
void LocalTable::termcheck(const string& f, int64_t* updates, double* currF2){
	VLOG(1) << "Start snapshot " << f;
//	Timer t;
	serializeToSnapshot(f, updates, currF2);
//	VLOG(1) << "Flushed snapshot " << f << " in: " << t.elapsed();

//  DLOG(INFO)<<getcallstack();
}

void LocalTable::start_checkpoint(const string& f){
	VLOG(1) << "Start checkpoint " << f;
//	Timer t;

	LocalTableCoder c(f, "wb");
	serializeStateToFile(&c);

	delta_file_ = new LocalTableCoder(f + ".delta", "wb");
	VLOG(1) << "End.";
//  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

void LocalTable::write_message(const KVPairData& put){
	if(!delta_file_)
		LOG(FATAL) << "W" << shard() << " delta file has closed";
	//DVLOG(1)<<"writing msg: "<<put.source()<<" to "<<shard()<<", epoch="<<put.epoch()<<" size="<<put.kv_data_size();
	for(int i = 0; i < put.kv_data_size(); ++i){
		//string k=put.kv_data(i).key(), v=put.kv_data(i).value();
		//DVLOG(1)<<"W"<<shard()<<" writing msg: "<<*(const int*)(k.data())<<" - "<<*(const float*)(v.data());
		delta_file_->WriteEntryToFile(put.kv_data(i).key(), put.kv_data(i).value(), "", "");
	}
}

void LocalTable::finish_checkpoint(){
	VLOG(1) << "FStart.";
	if(delta_file_){
		delete delta_file_;
		delta_file_ = nullptr;
	}
	VLOG(1) << "FEnd.";
}

void LocalTable::load_checkpoint(const string& f){
	if(!File::Exists(f)){
		LOG(FATAL) << "Skipping restore of non-existent shard " << f;
		return;
	}

	//TableData p;

	LocalTableCoder rf(f, "rb");
	string k, v1, v2, v3;
	while(rf.ReadEntryFromFile(&k, &v1, &v2, &v3)){
		update_str(k, v1, v2, v3);
	}

	// Replay delta log.
	LocalTableCoder df(f + ".delta", "rb");
	while(df.ReadEntryFromFile(&k, &v1, &v2, &v3)){
		update_str(k, v1, v2, v3);
	}
}

int64_t LocalTable::dump(const std::string& f, TableCoder* out)
{
	TableStateCoder* pc = dynamic_cast<TableStateCoder*>(out);
	pc->WriteHeader(f, shard(), size());
	serializeStateToFile(out);
	return size();
}

void LocalTable::restore(const std::string& f, TableCoder* in)
{
	string k, v1, v2, v3;
	TableStateCoder* pc = dynamic_cast<TableStateCoder*>(in);
	string type;
	int s;
	int64_t n;
	pc->ReadHeader(&type, &s, &n);
	CHECK_EQ(f, type) << "Archived shard type does not match";
	CHECK_EQ(shard(), s) << "Archived shard id does not match";
	for(int64_t i = 0; i < n; ++i){
		string k, v1, v2, v3;
		in->ReadEntryFromFile(&k, &v1, &v2, &v3);
		restoreState(k, v1, v2);
	}
}


//Dummy stub
//void LocalTable::DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) { return; }

} //namespace dsm
