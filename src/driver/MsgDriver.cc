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
bool MsgDriver::empty() const{
	return !inDisper.busy() && que.empty();
}
bool MsgDriver::busy() const{
	return inDisper.busy() || !outDisper.busy() || que.empty();
}

//Register
void MsgDriver::registerImmediateHandler(const int type, callback_t cb, bool spawnThread){
	inDisper.registerDispFun(type,cb,spawnThread);
}
void MsgDriver::unregisterImmediateHandler(const int type){
	inDisper.unregisterDispFun(type);
}
void MsgDriver::registerProcessHandler(const int type, callback_t cb, bool spawnThread){
	outDisper.registerDispFun(type,cb,spawnThread);
}
void MsgDriver::unregisterProcessHandler(const int type){
	outDisper.unregisterDispFun(type);
}
void MsgDriver::registerDefaultOutHandler(callback_t cb){
	defaultHandler=cb;
}

//reset & clear
void MsgDriver::resetImmediateHandler(){
	inDisper.clear();
}
void MsgDriver::resetProcessHandler(){
	outDisper.clear();
}
void MsgDriver::resetWaitingQueue(){
//	lock_guard<mutex> ql(lockQue);
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
size_t MsgDriver::abandonData(const int type){
//	lock_guard<mutex> ql(lockQue);
	size_t f=0,l=0;
	while(l<que.size()){
		if(que[l].second.tag==type){
			que[f++]=move(que[l++]);
		}else{
			++l;
		}
	}
	que.erase(que.begin()+f,que.end());
	return l-f;
}

//Process
bool MsgDriver::processInput(string& data, RPCInfo& info){
	if(!inDisper.receiveData(info.tag, data, info)){
//		lock_guard<mutex> ql(lockQue);
		que.push_back(make_pair(move(data),move(info)));
		return true;
	}
	return false;
}
bool MsgDriver::processOutput(string& data, RPCInfo& info){
	if(!outDisper.receiveData(info.tag, data, info)){
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
	string d;
	RPCInfo r;
	{
//		lock_guard<mutex> ql(lockQue);
		tie(d,r)=move(que.front());
		que.pop_front();
	}
	return processOutput(d, r);
}

} /* namespace dsm */
