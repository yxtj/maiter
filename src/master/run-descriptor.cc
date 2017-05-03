/*
 * run-descriptor.cc
 *
 *  Created on: Dec 28, 2015
 *      Author: tzhou
 */

#include "run-descriptor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(checkpoint_type,"CP_NONE","Type of checkpoint mechanism");
DEFINE_double(checkpoint_interval,0.0,"Interval of taking checkpoint(in second)");
DEFINE_int32(restore_from,-1,"The epoch to restore from (-1 for do not restore)");
//DEFINE_string(change_graph,"","The time and file-names of apply delta graphs."
//	" Format: \"<time(second)>:<filename>,<t2>:<f2>,<...>\""
//	" Example: '100:delta-1' means applying delta-1 at 100s; or '20:d1,50:d2' means applying d1 at 20s and than applying d2 at 50s.");

using namespace std;

namespace dsm{

void RunDescriptor::Init(const std::string& kernel,
		const std::string& method,
		GlobalTableBase *table,
		const bool checkpoint,
		const bool termcheck,
		const bool restore,
		const std::vector<int>& cp_tables)
{
	barrier = true;
	checkpoint_type = CP_NONE;
	if(checkpoint){
		if(checkpoint && !FLAGS_checkpoint_type.empty() &&
				!CheckpointType_Parse(FLAGS_checkpoint_type,&checkpoint_type)){
			LOG(FATAL)<<"Cannot understand given checkpoint type: "<<FLAGS_checkpoint_type;
		}
		checkpoint_interval = FLAGS_checkpoint_interval;
	}
	if(checkpoint_type != CP_NONE && checkpoint_interval<=0){
		LOG(FATAL)<<"Checkpoint interval is not given or is not positive.";
	}

	checkpoint_tables = cp_tables;
//	if(!checkpoint_tables.empty()){
//		checkpoint_type = CP_SYNC;
//	}

	this->termcheck=termcheck;

	if(restore && FLAGS_restore_from>=0){
		this->restore=true;
		this->restore_from_epoch=FLAGS_restore_from;
	}else{
		this->restore=false;
	}
	/*
	this->change_graph=!FLAGS_change_graph.empty();
	try{
		if(!FLAGS_change_graph.empty()){
			size_t plast=0,p=0;
			while(p!=string::npos){
				p=FLAGS_change_graph.find(',',plast);
				string group=FLAGS_change_graph.substr(plast,p==string::npos?string::npos:p-plast);
				size_t pc=group.find(':');
				if(pc==string::npos)
					throw invalid_argument("Cannot find ':' in change_graph.");
				delta_graph.emplace_back(stoi(group.substr(0,pc)),group.substr(pc+1));
				plast=p+1;
			}
		}
	}catch(exception& e){
		LOG(FATAL)<<"Error when parsing parameter change_graph: "<<e.what();
	}*/

	this->kernel = kernel;
	this->method = method;
	this->table = table;

}

}
