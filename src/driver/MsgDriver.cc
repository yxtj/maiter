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
MsgDriver::callback_t MsgDriver::GetDummyHandler(){
	static callback_t dummy=[](const std::string&, const RPCInfo&){};
	return dummy;
}
MsgDriver::MsgDriver():running_(false),net(nullptr)
{
	clear();
}

void MsgDriver::terminate(){
	running_=false;
}

//Register
void MsgDriver::linkInputter(NetworkThread* inputter){
	net=inputter;
}
void MsgDriver::registerImmediateHandler(const int type, callback_t cb, bool spawnThread){
	netDisper.registerDispFun(type,cb,spawnThread);
}
void MsgDriver::unregisterImmediateHandler(const int type){
	netDisper.unregisterDispFun(type);
}
void MsgDriver::registerProcessHandler(const int type, callback_t cb, bool spawnThread){
	queDisper.registerDispFun(type,cb,spawnThread);
}
void MsgDriver::unregisterProcessHandler(const int type){
	queDisper.unregisterDispFun(type);
}
void MsgDriver::registerDefaultOutHandler(callback_t cb){
	defaultHandler=cb;
}

//reset & clear
void MsgDriver::resetImmediateHandler(){
	netDisper.clear();
}
void MsgDriver::resetProcessHandler(){
	queDisper.clear();
}
void MsgDriver::resetWaitingQueue(){
	que.clear();
}
void MsgDriver::resetDefaultOutHandler(){
	defaultHandler=GetDummyHandler();
}
void MsgDriver::clear(){
	resetImmediateHandler();
	resetProcessHandler();
	resetWaitingQueue();
	resetDefaultOutHandler();
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
bool MsgDriver::processInput(string& data, RPCInfo& info){
	if(!netDisper.receiveData(info.tag, data, info)){
		que.push_back(make_pair(move(data),move(info)));
		return true;
	}
	return false;
}
bool MsgDriver::processOutput(string& data, RPCInfo& info){
	if(!queDisper.receiveData(info.tag, data, info)){
		defaultHandler(data,info);
		return true;
	}
	return false;
}

//Main working functions
bool MsgDriver::pushData(string& data, RPCInfo& info){
	return processInput(data,info);
}
bool MsgDriver::popData(){
	if(que.empty())	return false;
	pair<string, RPCInfo> t=move(que.front());
	que.pop_front();
	return processOutput(t.first, t.second);
}

void MsgDriver::delegated_run(){
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
			pushData(data,info);
			idled=false;
		}
		//output
		while(!que.empty()){
			pair<string, RPCInfo>& t=que.front();
			processOutput(t.first, t.second);
			que.pop_front();
			idled=false;
		}
		//sleep
		if(idled)
			Sleep();
	}
}

} /* namespace dsm */
