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
struct LocalTableCoder: public TableCoder{
	LocalTableCoder(const string &f, const string& mode);
	virtual ~LocalTableCoder();

	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
	virtual bool ReadEntryFromFile(string* k, string *v1, string *v2, string *v3);

	RecordFile *f_;
};

LocalTableCoder::LocalTableCoder(const string& f, const string &mode) :
		f_(new RecordFile(f, mode, RecordFile::LZO)){
}

LocalTableCoder::~LocalTableCoder(){
	delete f_;
}

bool LocalTableCoder::ReadEntryFromFile(string* k, string *v1, string *v2, string *v3){
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
void LocalTable::termcheck(const string& f, uint64_t* receives, uint64_t* updates, double* totalF2, uint64_t* defaultF2){
	VLOG(2) << "Start snapshot " << f;
//	Timer t;
	serializeToSnapshot(f, receives, updates, totalF2, defaultF2);
//	VLOG(1) << "Flushed snapshot " << f << " in: " << t.elapsed();

//  DLOG(INFO)<<getcallstack();
}

void LocalTable::start_checkpoint(const string& f){
	VLOG(2) << "Start checkpoint " << f;
//	Timer t;

	LocalTableCoder c(f, "w");
	serializeToFile(&c);

	delta_file_ = new LocalTableCoder(f + ".delta", "w");
	VLOG(1) << "End.";
	//  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

void LocalTable::write_message(const KVPairData& put){
//	DVLOG(1)<<"writing msg: "<<put.source()<<" to "<<id()<<", epoch="<<put.epoch()<<" size="<<put.kv_data_size();
	for(int i = 0; i < put.kv_data_size(); ++i){
//		string k=put.kv_data(i).key(), v=put.kv_data(i).value();
//		DVLOG(1)<<"writing msg: "<<*(const int*)(k.data())<<" - "<<*(const float*)(v.data());
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

void LocalTable::restore(const string& f){
	if(!File::Exists(f)){
		VLOG(1) << "Skipping restore of non-existent shard " << f;
		return;
	}

	TableData p;

	LocalTableCoder rf(f, "r");
	string k, v1, v2, v3;
	while(rf.ReadEntryFromFile(&k, &v1, &v2, &v3)){
		update_str(k, v1, v2, v3);
	}

	// Replay delta log.
	LocalTableCoder df(f + ".delta", "r");
	while(df.ReadEntryFromFile(&k, &v1, &v2, &v3)){
		update_str(k, v1, v2, v3);
	}
}

//Dummy stub
//void LocalTable::DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) { return; }

} //namespace dsm
