#include <gflags/gflags.h>
#include <glog/logging.h>
#include "NetworkThread2.h"
#include "NetworkImplMPI.h"
#include "Task.h"
#include "util/common.h"
#include <string>
#include <thread>
#include <chrono>

#include "msg/message.pb.h"

//DECLARE_bool(localtest);
DECLARE_double(sleep_time);
//DEFINE_bool(rpc_log, false, "");

using namespace std;

namespace dsm {

static inline void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

NetworkThread2::NetworkThread2() :
		running(false), net(NULL){
	net = NetworkImplMPI::GetInstance();
	for(int i = 0; i < kMaxMethods; ++i){
		callbacks_[i] = NULL;
	}

	running = true;
	t_ = thread(&NetworkThread2::Run, this);
	t_.detach();
}

int NetworkThread2::id() const{
	return net->id();
}
int NetworkThread2::size() const{
	return net->size();
}

bool NetworkThread2::active() const{
	return pending_sends_.size() > 0 || net->unconfirmedTaskNum() > 0;
}

int64_t NetworkThread2::pending_bytes() const{
	int64_t t = net->unconfirmedBytes();

	lock_guard<recursive_mutex> sl(ps_lock);
	for(size_t i = 0; i < pending_sends_.size(); ++i){
		t += pending_sends_[i]->payload.size();
	}

	return t;
}

int NetworkThread2::waiting_messages() const{
	return receive_buffer.size();
}

void NetworkThread2::ProcessReceivedMsg(int source, int tag, string& data){
	//Case 1: received a reply packet, put into reply buffer
	//Case 2: received a RPC request, call related callback function
	//Case 3: received a normal data packet, put into received buffer
	//Case 1
//	if(tag==MTYPE_REPLY){
//		ReplyMessage rm;
//		rm.ParseFromArray(data.data(),data.size());
//		tag=rm.type();
//		VLOG(2) << "Processing reply, type " << tag << ", from " << source << ", to " << id();
//		lock_guard<recursive_mutex> sl(rep_lock[tag]);
//		reply_buffer[tag][source].push_back(data);
//	}else{
//		if(callbacks_[tag] != NULL){
//			//Case 2
//			CallbackInfo *ci = callbacks_[tag];
////			Task::Decode(*(ci->req),data);
////			VLOG(2) << "Processing RPC, type " << tag << ", from " << source << ", to " << id()
////								<< ", content:" << ci->req->ShortDebugString();
//
//			RPCInfo rpc = { source, id(), tag };
//			if(ci->spawn_thread){
//				thread t(bind(&NetworkThread2::InvokeCallback, this, ci, data, rpc));
//				t.detach();	//without it, the function is terminated when t is deconstructed.
//			}else{
//				InvokeCallback(ci, data, rpc);
//			}
//		}else{
			//Case 3
//			lock_guard<recursive_mutex> sl(rec_lock[tag]);
//			receive_buffer[tag][source].push_back(data);
			lock_guard<recursive_mutex> sl(rec_lock);
			receive_buffer.push_back(make_pair(move(data),TaskBase{source,tag}));
//		}
//	}
}

void NetworkThread2::Run(){
//	double t=Now();
	while(running){
		//receive
		TaskHeader hdr;
		if(net->probe(&hdr)){
			string data = net->receive(&hdr);
//			DLOG_IF(INFO,hdr.type!=4)<<"Receive(t) from "<<hdr.src_dst<<" to "<<id()<<", type "<<hdr.type;
			stats["received bytes"] += hdr.nBytes;
			stats["received type." + to_string(hdr.type)] += 1;
			CHECK_LT(hdr.src_dst, kMaxHosts);

			ProcessReceivedMsg(hdr.src_dst, hdr.type, data);
//			if(Now()-t>12. && hdr.type!=4){
//				string s=receiveQueueOccupation();
//				DLOG_IF(INFO,!s.empty())<<"on "<<id()<<" receive buffer:\n"<<s;
//			}
		}else{
			Sleep();
		}
		//clear useless send buffer
		net->collectFinishedSend();
		//send
//		Timer tt;
		/* bunch send: */
		if(!pending_sends_.empty()){
			//TODO: use two-buffers-swapping to implement this for better performance
			lock_guard<recursive_mutex> sl(ps_lock);
//			DLOG_IF(INFO,Now()-t>12.)<<id()<<" pending # : "<<pending_sends_.size();
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

bool NetworkThread2::checkReceiveQueue(std::string& data, TaskBase& info){
	if(!receive_buffer.empty()){
		lock_guard<recursive_mutex> sl(rec_lock);
		if(receive_buffer.empty()) return false;

		tie(data,info)=receive_buffer.front();

		receive_buffer.pop_front();
		return true;
	}
	return false;
}

void NetworkThread2::ReadAny(string& data, int *srcRet, int *typeRet){
	Timer t;
	while(!TryReadAny(data, srcRet, typeRet)){
		Sleep();
	}
	stats["network_time"] += t.elapsed();
}
bool NetworkThread2::TryReadAny(string& data, int *srcRet, int *typeRet){
	TaskBase info;
	if(checkReceiveQueue(data,info)){
		if(srcRet) *srcRet = info.src_dst;
		if(typeRet) *typeRet = info.type;
		return true;
	}
	return false;
}

// Enqueue the given request to pending buffer for transmission.
inline int NetworkThread2::Send(Task *req){
//	DLOG_IF(INFO,req->type!=4)<<"Sending(t) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type;
	int size = req->payload.size();
	stats["sent bytes"] += size;
	stats["sent type." + to_string(req->type)] += 1;
	Timer t;
	lock_guard<recursive_mutex> sl(ps_lock);
//	DLOG_IF(INFO,t.elapsed()>0.005)<<"Sending(l) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type<<", lock time="<<t.elapsed();
	pending_sends_.push_back(req);
	return size;
}
int NetworkThread2::Send(int dst, int method, const Message &msg){
	return Send(new Task(dst, method, msg, MsgHeader()));
}

// Directly (Physically) send the request.
inline int NetworkThread2::DSend(Task *req){
	int size = req->payload.size();
	net->send(req);
	return size;
}
int NetworkThread2::DSend(int dst, int method, const Message &msg){
	return DSend(new Task(dst, method, msg));
}

void NetworkThread2::Shutdown(){
	if(running){
		Flush();
		running = false;
		net->shutdown();
	}
}

void NetworkThread2::Flush(){
	while(active()){
		Sleep();
	}
}

void NetworkThread2::Broadcast(int method, const Message& msg){
	int myid = id();
	for(int i = 0; i < net->size(); ++i){
		if(i != myid)
			Send(i, method, msg);
	}
}

void NetworkThread2::SyncBroadcast(int method, const Message& msg){
	VLOG(2) << "Sending: " << msg.ShortDebugString();
	Broadcast(method, msg);
	WaitForSync(method, net->size() - 1);
}

void NetworkThread2::WaitForSync(int method, int count){
//	vector<bool> replied(size(), false);
//	while(count > 0){
//		for(int i = 0; i < net->size(); ++i){
//			if(replied[i] == false && checkReplyQueue(i, method, nullptr)){
//				--count;
//				replied[i] = true;
//			}
//		}
//		Sleep();
//	}
}

static NetworkThread2* self = NULL;
NetworkThread2* NetworkThread2::Get(){
	return self;
}

static void ShutdownImpl(){
	NetworkThread2::Get()->Shutdown();
}

void NetworkThread2::Init(){
	VLOG(1) << "Initializing network...";
	CHECK(self == NULL);
	self = new NetworkThread2();
	atexit(&ShutdownImpl);
}

}

