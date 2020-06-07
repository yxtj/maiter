/*
 * table-coder.h
 *
 *  Created on: Feb 18, 2016
 *      Author: tzhou
 */

#ifndef TABLE_TABLE_CODER_H_
#define TABLE_TABLE_CODER_H_

#include "table.h"

namespace dsm{

// Interface for serializing tables, either to disk or for transmitting via RPC.
struct TableCoder{
	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3) = 0;
	virtual bool ReadEntryFromFile(std::string* k, std::string *v1, std::string *v2, std::string *v3) = 0;

	virtual ~TableCoder(){}
};

// Interface for serializing messages, either to disk or for transmitting via RPC.
struct KVPairCoder{
	virtual void WriteEntryToNet(StringPiece k, StringPiece v1) = 0;
	virtual bool ReadEntryFromNet(std::string* k, std::string *v1) = 0;

	virtual ~KVPairCoder(){}
};


struct DecodeIteratorBase {};

// Added for the sake of triggering on remote updates/puts <CRM>
template <typename K, typename V1, typename V2, typename V3>
struct FileDecodeIterator :
		public TypedTableIterator<K, V1, V2, V3>, public DecodeIteratorBase
{
	Marshal<K>* kmarshal(){
		return nullptr;
	}
	Marshal<V1>* v1marshal(){
		return nullptr;
	}
	Marshal<V2>* v2marshal(){
		return nullptr;
	}
	Marshal<V3>* v3marshal(){
		return nullptr;
	}

	FileDecodeIterator(){
		clear();
		rewind();
	}
	void append(K k, V1 v1, V2 v2, V3 v3){
		ClutterRecord<K, V1, V2, V3> thispair(k, v1, v2, v3);
		decodedeque.push_back(thispair);
//    LOG(ERROR) << "APPEND";
	}
	void clear(){
		decodedeque.clear();
//    LOG(ERROR) << "CLEAR";
	}
	void rewind(){
		intit = decodedeque.begin();
//    LOG(ERROR) << "REWIND: empty? " << (intit == decodedeque.end());
	}
	bool done(){
		return intit == decodedeque.end();
	}
	bool Next(){
		intit++;
		return true;
	}
	const K& key(){
		static K k2;
		if(intit != decodedeque.end()){
			k2 = intit->k;
		}
		return k2;
	}
	V1& value1(){
		static V1 vv;
		if(intit != decodedeque.end()){
			vv = intit->v1;
		}
		return vv;
	}
	V2& value2(){
		static V2 vv;
		if(intit != decodedeque.end()){
			vv = intit->v2;
		}
		return vv;
	}
	V3& value3(){
		static V3 vv;
		if(intit != decodedeque.end()){
			vv = intit->v3;
		}
		return vv;
	}

private:
	std::vector<ClutterRecord<K, V1, V2, V3> > decodedeque;
	typename std::vector<ClutterRecord<K, V1, V2, V3> >::iterator intit;
};

template <typename K, typename V1>
struct NetDecodeIterator :
		public PTypedTableIterator<K, V1>, public DecodeIteratorBase
{
	Marshal<K>* kmarshal(){
		return nullptr;
	}
	Marshal<V1>* v1marshal(){
		return nullptr;
	}

	NetDecodeIterator(){
		clear();
		rewind();
	}
	void append(K k, V1 v1){
		std::pair<K, V1> thispair(k, v1);
		decodedeque.push_back(thispair);
//    LOG(ERROR) << "APPEND";
	}
	void clear(){
		decodedeque.clear();
//    LOG(ERROR) << "CLEAR";
	}
	void rewind(){
		intit = decodedeque.begin();
//    LOG(ERROR) << "REWIND: empty? " << (intit == decodedeque.end());
	}
	bool done(){
		return intit == decodedeque.end();
	}
	bool Next(){
		++intit;
		return true;
	}
	const K& key(){
		static K k2;
		if(intit != decodedeque.end()){
			k2 = intit->first;
		}
		return k2;
	}
	V1& value1(){
		static V1 vv;
		if(intit != decodedeque.end()){
			vv = intit->second;
		}
		return vv;
	}

private:
	std::vector<std::pair<K, V1> > decodedeque;
	typename std::vector<std::pair<K, V1> >::iterator intit;
};

class RecordFile;
// Encodes or decodes table entries, reading and writing from the specified file.
struct TableStateCoder : public TableCoder{
	TableStateCoder(const std::string& f, const std::string& mode);
	virtual ~TableStateCoder();

	virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
	virtual bool ReadEntryFromFile(std::string* k, std::string* v1, std::string* v2, std::string* v3);

	void WriteHeader(StringPiece type, const int shard, const int64_t n);
	void ReadHeader(std::string* type, int *shard, int64_t* n);

	RecordFile* f_;
};


}

#endif /* TABLE_TABLE_CODER_H_ */
