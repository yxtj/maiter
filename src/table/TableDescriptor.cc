/*
 * TableDescriptor.cc
 *
 *  Created on: Dec 14, 2015
 *      Author: tzhou
 */

#include "TableDescriptor.h"

#include "table/tbl_widget/sharder.h"
#include "table/tbl_widget/term_checker.h"
#include "table/tbl_widget/trigger.h"
#include "table/tbl_widget/IterateKernel.h"
#include "util/marshal.hpp"
#include "table.h"	//for TableFactory
#include "TableHelper.h"

namespace dsm{

void TableDescriptor::init(){
	table_id = -1;
	num_shards = -1;
	max_stale_time = 0.;

	helper = nullptr;
	partition_factory = deltaT_factory= nullptr;
	key_marshal = value1_marshal = value2_marshal = value3_marshal = nullptr;
	sharder = nullptr;
	iterkernel = nullptr;
	termchecker = nullptr;
}

void TableDescriptor::reset(){
	if(key_marshal)	delete key_marshal;
	if(value1_marshal) delete value1_marshal;
	if(value2_marshal) delete value2_marshal;
	if(value3_marshal) delete value3_marshal;
	if(partition_factory) delete partition_factory;
	if(deltaT_factory) delete deltaT_factory;
	if(sharder) delete sharder;
	if(iterkernel) delete iterkernel;
	if(termchecker) delete termchecker;
	for(size_t i=0; i<triggers.size(); ++i)
		delete triggers[i];
	triggers.clear();
	if(helper) delete helper;

	init();
}

} //namespace dsm
