/*
 * MsgDriver.cpp
 *
 *  Created on: Dec 15, 2015
 *      Author: tzhou
 */

#include "MsgDriver.h"
#include "net/NetworkThread.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thread>

DECLARE_double(sleep_time);

using namespace std;

namespace dsm {

void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

MsgDriver::MsgDriver():running_(true),net(nullptr)
{

}

void MsgDriver::registerNetDispFun(const int type, cb_net_t cb){
	netDisper.registerDispFun(type,cb);
}
void MsgDriver::registerQueDispFun(const int type, cb_que_t cb){
	queDisper.registerDispFun(type,cb);
}
void MsgDriver::linkInputter(NetworkThread* inputter){
	net=inputter;
}

//bool MsgDriver::DecodeMessage(const int type, const std::string& data, Message* msg){
//	msg->
//	return false;
//}

void MsgDriver::readBlocked(string& msg, RPCInfo& info){
	info.dest=net->id();
//	net->Read(Task::ANY_SRC, Task::ANY_TYPE, &msg, &info.source, &info.tag);
	net->ReadAny(msg, &info.source, &info.tag);
}
bool MsgDriver::readUnblocked(string& msg, RPCInfo& info){
	info.dest=net->id();
//	net->TryRead(Task::ANY_SRC, Task::ANY_TYPE, &msg, &info.source, &info.tag);
	return net->TryReadAny(msg, &info.source, &info.tag);
}

void MsgDriver::handleInput(string& data, RPCInfo& info){
	if(!netDisper.receiveData(info.tag, data, info))
		que.push(make_pair(info.tag, data));
}
void MsgDriver::handleOutput(const string& data, const int type){
	if(!queDisper.receiveData(type, data))
		defaultHandler(data);
}

void MsgDriver::run(){
	string data;
	RPCInfo info;
	while(running_){
		//TODO: use 2 thread to handle input and output
		bool idled=true;
		while(readUnblocked(data,info)){
			handleInput(data,info);
			idled=false;
		}

		//output
		while(!que.empty()){
			const pair<int,string>& t=que.front();
			handleOutput(t.second, t.first);
			idled=false;
		}
		if(idled)
			Sleep();
	}
}

} /* namespace dsm */
