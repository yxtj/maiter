/*
 * Task.cpp
 *
 *  Created on: Nov 30, 2015
 *      Author: tzhou
 */

#include "Task.h"
#include <google/protobuf/message.h>

namespace dsm {

Task::Task(int s_d,int type,const google::protobuf::Message& msg,const MsgHeader& h):
		Task(s_d,type)
{
	const char* p=reinterpret_cast<const char*>(&h);
	payload.append(p, p+sizeof(h));
	msg.AppendToString(&payload);
}

} /* namespace dsm */
