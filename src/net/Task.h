/*
 * Task.h
 *
 *  Created on: Nov 29, 2015
 *      Author: tzhou
 */

#ifndef NET_TASK_H_
#define NET_TASK_H_

#include <string>
//#include <memory>
#include "util/timer.h"
//#include <google/protobuf/message.h>
namespace google{
namespace protobuf{
class Message;
}
}

namespace dsm {

struct MsgHeader{
	MsgHeader(const bool reply=false):is_reply(reply){}
	bool is_reply;
};

struct TaskBase{
	int src_dst;
	int type;
	static constexpr int ANY_SRC=-1;
	static constexpr int ANY_TYPE=-1;
};

struct TaskHeader :public TaskBase {
	int nBytes;
};

struct Task : public TaskBase{
	std::string payload;
	Task(int s_d,int type):TaskBase{s_d,type} {}//src_dst(s_d),type(type){}
	Task(int s_d,int type,std::string&& s):TaskBase{s_d,type},payload(s){}
	Task(int s_d,int type,const std::string& s):TaskBase{s_d,type},payload(s){}

	Task(int s_d,int type,const google::protobuf::Message& msg,const MsgHeader& h=MsgHeader(false));

	static void Decode(google::protobuf::Message& msg, const std::string& data);
	void decode(google::protobuf::Message& msg);
};

inline void Task::decode(google::protobuf::Message& msg){
	Task::Decode(msg,payload);
}

struct TaskTimed : public Task{
	Timer t;
	TaskTimed(int s_d,int type):Task(s_d,type){}
	TaskTimed(int s_d,int type,std::string&& s):Task(s_d,type,s){}
	TaskTimed(int s_d,int type,const std::string& s):Task(s_d,type,s){}

	TaskTimed(int s_d,int type,const google::protobuf::Message& msg,const MsgHeader& h=MsgHeader(false)):Task(s_d,type,msg,h){}
};

//typedef std::shared_ptr<Task> Task_ptr;

} /* namespace dsm */

//namespace std{
//template<>
//struct hash<dsm::Task>{
//	typedef dsm::Task argument_type;
//	typedef std::size_t result_type;
//	result_type operator()(const argument_type& t) const{
//		return std::hash<int>()(t.src_dst)^(std::hash<int>()(t.type)<<2);//^(std::hash(t.payload)<<4);
//	}
//};
//}

#endif /* NET_TASK_H_ */
