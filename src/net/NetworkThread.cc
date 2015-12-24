#include <gflags/gflags.h>
#include <glog/logging.h>
#include "NetworkThread.h"
#include "NetworkImplMPI.h"
#include "Task.h"
#include "util/common.h"
#include <string>
#include <thread>
#include <chrono>

DECLARE_double(sleep_time);

using namespace std;

namespace dsm {

static inline void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

NetworkThread::NetworkThread() :
		running(false), net(NULL){
	net = NetworkImplMPI::GetInstance();

	running = true;
	t_ = thread(&NetworkThread::Run, this);
	t_.detach();
}

int NetworkThread::id() const{
	return net->id();
}
int NetworkThread::size() const{
	return net->size();
}

bool NetworkThread::active() const{
	return pending_sends_.size() > 0 || net->unconfirmedTaskNum() > 0;
}
size_t NetworkThread::pending_pkgs() const{
	return net->unconfirmedTaskNum()+pending_sends_.size();
}
int64_t NetworkThread::pending_bytes() const{
	int64_t t = net->unconfirmedBytes();

	lock_guard<recursive_mutex> sl(ps_lock);
	for(size_t i = 0; i < pending_sends_.size(); ++i){
		t += pending_sends_[i]->payload.size();
	}

	return t;
}

size_t NetworkThread::unpicked_pkgs() const{
	return receive_buffer.size();
}
int64_t NetworkThread::unpicked_bytes() const{
	int64_t t=0;
	lock_guard<recursive_mutex> rl(rec_lock);
	for(const auto& p: receive_buffer){
		t+=p.first.size();
	}
	return t;
}

//void NetworkThread::ProcessReceivedMsg(int source, int tag, string& data){
//	receive_buffer.push_back(make_pair(move(data),TaskBase{source,tag}));
//}

void NetworkThread::Run(){
	while(running){
		//receive
		TaskHeader hdr;
		if(net->probe(&hdr)){
			string data = net->receive(&hdr);
//			DLOG_IF(INFO,hdr.type!=4)<<"Receive(t) from "<<hdr.src_dst<<" to "<<id()<<", type "<<hdr.type;
			stats["received bytes"] += hdr.nBytes;
			stats["received type." + to_string(hdr.type)] += 1;

//			ProcessReceivedMsg(hdr.src_dst, hdr.type, data);
			receive_buffer.push_back(make_pair(move(data),TaskBase{hdr.src_dst, hdr.type}));
		}else{
			Sleep();
		}
		//clear useless send buffer
		net->collectFinishedSend();
		//send
		/* bunch send: */
		if(!pending_sends_.empty()){
			//TODO: use two-buffers-swapping to implement this for better performance
			lock_guard<recursive_mutex> sl(ps_lock);
			for(auto it = pending_sends_.begin(); it != pending_sends_.end(); ++it)
				net->send(*it);
			pending_sends_.clear();
		}
		/* single send: */
//		while(!pending_sends_.empty()){
//			lock_guard<recursive_mutex> sl(ps_lock);
//			Task* s = pending_sends_.back();
//			pending_sends_.pop_back();
////		sl.~lock_guard();
//			net->send(s);
//		}
	}
}

bool NetworkThread::checkReceiveQueue(std::string& data, TaskBase& info){
	if(!receive_buffer.empty()){
		lock_guard<recursive_mutex> sl(rec_lock);
		if(receive_buffer.empty()) return false;

		tie(data,info)=receive_buffer.front();

		receive_buffer.pop_front();
		return true;
	}
	return false;
}

void NetworkThread::ReadAny(string& data, int *srcRet, int *typeRet){
	Timer t;
	while(!TryReadAny(data, srcRet, typeRet)){
		Sleep();
	}
	stats["network_time"] += t.elapsed();
}
bool NetworkThread::TryReadAny(string& data, int *srcRet, int *typeRet){
	TaskBase info;
	if(checkReceiveQueue(data,info)){
		if(srcRet) *srcRet = info.src_dst;
		if(typeRet) *typeRet = info.type;
		return true;
	}
	return false;
}

// Enqueue the given request to pending buffer for transmission.
inline int NetworkThread::Send(Task *req){
//	DLOG_IF(INFO,req->type!=4)<<"Sending(t) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type;
	int size = req->payload.size();
	stats["sent bytes"] += size;
	stats["sent type." + to_string(req->type)] += 1;
//	Timer t;
	lock_guard<recursive_mutex> sl(ps_lock);
//	DLOG_IF(INFO,t.elapsed()>0.005)<<"Sending(l) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type<<", lock time="<<t.elapsed();
	pending_sends_.push_back(req);
	return size;
}
int NetworkThread::Send(int dst, int method, const Message &msg){
	return Send(new Task(dst, method, msg));
}

// Directly (Physically) send the request.
inline int NetworkThread::DSend(Task *req){
	int size = req->payload.size();
	net->send(req);
	return size;
}
int NetworkThread::DSend(int dst, int method, const Message &msg){
	return DSend(new Task(dst, method, msg));
}

void NetworkThread::Shutdown(){
	if(running){
		Flush();
		running = false;
		net->shutdown();
	}
}

void NetworkThread::Flush(){
	while(active()){
		Sleep();
	}
}

void NetworkThread::Broadcast(int method, const Message& msg){
	net->broadcast(new Task(Task::ANY_SRC, method, msg));
//	int myid = id();
//	for(int i = 0; i < net->size(); ++i){
//		if(i != myid)
//			Send(i, method, msg);
//	}
}

static NetworkThread* self = NULL;
NetworkThread* NetworkThread::Get(){
	return self;
}

static void ShutdownImpl(){
	NetworkThread::Get()->Shutdown();
}

void NetworkThread::Init(){
	VLOG(1) << "Initializing network...";
	CHECK(self == NULL);
	self = new NetworkThread();
	atexit(&ShutdownImpl);
}

} //namespace dsm

