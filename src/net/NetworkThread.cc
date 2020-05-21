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
		running(false), done(false), net(nullptr){
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
	return net->unconfirmedTaskNum() > 0 ||
			ps_buffer_[0].size() > 0 || ps_buffer_[1].size() > 0;
}
size_t NetworkThread::pending_pkgs() const{
	return net->unconfirmedTaskNum()+ps_buffer_[0].size()+ps_buffer_[1].size();
}
int64_t NetworkThread::pending_bytes() const{
	int64_t t = net->unconfirmedBytes();

	lock_guard<recursive_mutex> sl(ps_lock);
	for(const vector<Task*>& vec:ps_buffer_){
		for(Task* p:vec)
			t+=p->payload.size();
	}

	return t;
}

//size_t NetworkThread::unpicked_pkgs() const{
//	return receive_buffer.size();
//}
int64_t NetworkThread::unpicked_bytes() const{
	int64_t t=0;
	lock_guard<recursive_mutex> rl(rec_lock);
	for(const auto& p: receive_buffer){
		t+=p.first.size();
	}
	return t;
}

void NetworkThread::Run(){
	TaskHeader hdr;
	unsigned cnt_idle_loop=0;
	static constexpr unsigned SLEEP_CNT=256;
	done=false;
	while(running){
		bool idle=true;
		//receive
		if(!pause_ && net->probe(&hdr)){
			string data = net->receive(&hdr);
			VLOG_IF(2,hdr.type!=4)<<"Receive(t) from "<<hdr.src_dst<<" to "<<id()<<", type "<<hdr.type;
			lock_guard<recursive_mutex> sl(rec_lock);
			stats["received bytes"] += hdr.nBytes;
			stats["received type." + to_string(hdr.type)] += 1;

			receive_buffer.push_back(make_pair(move(data),TaskBase{hdr.src_dst, hdr.type}));
			idle=false;
		}
		//clear useless send buffer
		net->collectFinishedSend();
		//send
		/* bunch send: */
		if(!pause_ && !pending_sends_->empty()){
			//two-buffers-swapping implementation for better performance
			vector<Task*>* pv;
			{
				lock_guard<recursive_mutex> sl(ps_lock);
				pv = pending_sends_;
				pending_sends_=&ps_buffer_[ps_idx_++%2];
			}
			auto end_it=pv->end();
			for(auto it = pv->begin(); it != end_it; ++it)
				net->send(*it);
			pv->clear();
		}else{
			if(idle && ++cnt_idle_loop%SLEEP_CNT==0)
				Sleep();
		}
	}
	done=true;
}

bool NetworkThread::checkReceiveQueue(std::string& data, TaskBase& info){
//	TaskHeader hdr;
//	if(net->probe(&hdr)){
//		data = net->receive(&hdr);
//		VLOG_IF(2,hdr.type!=4)<<"Receive(t) from "<<hdr.src_dst<<" to "<<id()<<", type "<<hdr.type;
//		lock_guard<recursive_mutex> sl(rec_lock);
//		stats["received bytes"] += hdr.nBytes;
//		stats["received type." + to_string(hdr.type)] += 1;
//		info=hdr;
////		info.src_dst=hdr.src_dst;
////		info.type=hdr.type;
//
//		return true;
//	}
//	return false;
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
		VLOG_IF(2,info.type!=4)<<"Receive(f) from "<<info.src_dst<<" to "<<id()<<", type "<<info.type;
		if(srcRet) *srcRet = info.src_dst;
		if(typeRet) *typeRet = info.type;
		return true;
	}
	return false;
}

// Enqueue the given request to pending buffer for transmission.
inline int NetworkThread::Send(Task *req){
	VLOG_IF(2,req->type!=4)<<"Sending(t) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type;
	int size = req->payload.size();
	stats["sent bytes"] += size;
	stats["sent type." + to_string(req->type)] += 1;
//	Timer t;
	lock_guard<recursive_mutex> sl(ps_lock);
//	DLOG_IF(INFO,t.elapsed()>0.005)<<"Sending(l) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type<<", lock time="<<t.elapsed();
	pending_sends_->push_back(req);
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

NetworkThread* NetworkThread::self = nullptr;
NetworkThread* NetworkThread::Get(){
	if(self==nullptr){
		self=new NetworkThread();
	}
	return self;
}

void NetworkThread::Init(int argc, char* argv[])
{
	VLOG(1) << "Initializing network...";
	NetworkImplMPI::Init(argc, argv);
	self = new NetworkThread();
	atexit(&NetworkThread::Shutdown);
}

void NetworkThread::Shutdown(){
	if(self != nullptr) {
		NetworkThread* p = nullptr;
		swap(self, p); // use the swap primitive to preform safe deletion
		if(p->running) {
			p->Flush();	//finish all the sending
			p->running = false;
			//wait for Run() to exit
			while(!p->done) {
				Sleep();
			}
			p->t_.join();
			p->net = nullptr;
			NetworkImplMPI::Shutdown();
		}
		delete p;
	}
}

} //namespace dsm

