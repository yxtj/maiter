#include "table-coder.h"
#include "table-coder.h"
#include "table-coder.h"
#include "util/file.h"

using namespace std;

namespace dsm {

TableStateCoder::TableStateCoder(const string& f, const string& mode) :
	f_(new RecordFile(f, mode, RecordFile::NONE))
{
}

TableStateCoder::~TableStateCoder(){
	delete f_;
}

void TableStateCoder::WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3)
{
	f_->writeChunk(k);
	f_->writeChunk(v1);
	f_->writeChunk(v2);
	f_->writeChunk(v3);
}

bool TableStateCoder::ReadEntryFromFile(string* k, string* v1, string* v2, string* v3)
{
	if(f_->readChunk(k)){
		f_->readChunk(v1);
		f_->readChunk(v2);
		f_->readChunk(v3);
		return true;
	}

	return false;
}

void TableStateCoder::WriteHeader(StringPiece type, const int shard, const int64_t n)
{
	StringMarshal<int> ms;
	StringMarshal<int64_t> mn;
	f_->writeChunk(type);
	string s;
	ms.marshal(shard, &s);
	f_->writeChunk(s);
	s.clear();
	mn.marshal(n, &s);
	f_->writeChunk(s);
}

void TableStateCoder::ReadHeader(std::string* type, int* shard, int64_t* n)
{
	StringMarshal<int> ms;
	StringMarshal<int64_t> mn;
	string s;
	f_->readChunk(type);
	f_->readChunk(&s);
	ms.unmarshal(s, shard);
	f_->readChunk(&s);
	mn.unmarshal(s, n);
}

}
