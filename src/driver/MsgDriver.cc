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

//Helper
void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

MsgDriver::MsgDriver():running_(false),net(nullptr)
{

}

void MsgDriver::terminate(){
	running_=false;
}

//Register
void MsgDriver::linkInputter(NetworkThread* inputter){
	net=inputter;
}
void MsgDriver::registerImmediateHandler(const int type, callback_t cb){
	netDisper.registerDispFun(type,cb);
}
void MsgDriver::registerProcessHandler(const int type, callback_t cb){
	queDisper.registerDispFun(type,cb);
}
void MsgDriver::registerDefaultOutHandler(callback_t cb){
	defaultHandler=cb;
}

//Read data
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

//Process
void MsgDriver::processInput(string& data, RPCInfo& info){
	if(!netDisper.receiveData(info.tag, data, info))
		que.push(make_pair(move(data),move(info)));
}
void MsgDriver::processOutput(const string& data, const RPCInfo& info){
	if(!queDisper.receiveData(info.tag, data, info))
		defaultHandler(data);
}

//Main working process
void MsgDriver::run(){
	if(net==nullptr){
		LOG(FATAL)<<"input source has not been set.";
	}
	running_=true;
	//TODO: use 2 thread to handle input and output
	string data;
	RPCInfo info;
	while(running_){
		bool idled=true;
		//input
		while(readUnblocked(data,info)){
			processInput(data,info);
			idled=false;
		}
		//output
		while(!que.empty()){
			const pair<string, RPCInfo>& t=que.front();
			processOutput(t.first, t.second);
			que.pop();
			idled=false;
		}
		//sleep
		if(idled)
			Sleep();
	}
}

} /* namespace dsm */
