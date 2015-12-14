#include <gflags/gflags.h>
#include <glog/logging.h>
#include "NetworkThread.h"
#include "NetworkImplMPI.h"
#include "Task.h"
#include "util/common.h"
//#include "util/common.pb.h"
#include <string>
#include <thread>
#include <chrono>

//DECLARE_bool(localtest);
DECLARE_double(sleep_time);
//DEFINE_bool(rpc_log, false, "");

using namespace std;

namespace dsm {

static inline void Sleep(){
	this_thread::sleep_for(chrono::duration<double>(FLAGS_sleep_time));
}

NetworkThread::NetworkThread():running(false),net(NULL){
	net=NetworkImplMPI::GetInstance();
	for(int i = 0; i < kMaxMethods; ++i){
		callbacks_[i] = NULL;
	}

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
	return pending_sends_.size() > 0 || net->unconfirmedTaskNum()>0;
}

int64_t NetworkThread::pending_bytes() const{
	int64_t t = net->unconfirmedBytes();

//	boost::recursive_mutex::scoped_lock sl(ps_lock);
	lock_guard<recursive_mutex> sl(ps_lock);
	for(size_t i = 0; i < pending_sends_.size(); ++i){
		t += pending_sends_[i]->payload.size();
	}

	return t;
}


void NetworkThread::InvokeCallback(CallbackInfo *ci, RPCInfo rpc){
	ci->call(rpc);
	MsgHeader reply_header(true);
	Send(new Task(rpc.source, rpc.tag, *ci->resp,reply_header));
}
void NetworkThread::ProcessReceivedMsg(int source, int tag, string& data){
	//Case 1: received a reply packet, put into reply buffer
	//Case 2: received a RPC request, call related callback function
	//Case 3: received a normal data packet, put into received buffer
	const MsgHeader *h = reinterpret_cast<const MsgHeader*>(data.data());
	if(h->is_reply){	//Case 1
		//boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
		VLOG(2)<<"Processing reply, type "<<tag<<", from "<<source<<", to "<<id();
		lock_guard<recursive_mutex> sl(rep_lock[tag]);
		reply_buffer[tag][source].push_back(data);
	}else{
		if(callbacks_[tag] != NULL){	//Case 2
			CallbackInfo *ci = callbacks_[tag];
			ci->req->ParseFromArray(data.data() + sizeof(MsgHeader), data.size() - sizeof(MsgHeader));
			VLOG(2) << "Processing RPC, type "<<tag<<", from "<<source<<", to "<<id()<<", content:" << ci->req->ShortDebugString();

			RPCInfo rpc = { source, id(), tag };
			if(ci->spawn_thread){
				thread t(bind(&NetworkThread::InvokeCallback, this, ci, rpc));
				t.detach();	//without it, the function is terminated when t is deconstructed.
			}else{
				InvokeCallback(ci, rpc);
			}
		}else{	//Case 3
			//boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
			lock_guard<recursive_mutex> sl(rec_lock[tag]);
			receive_buffer[tag][source].push_back(data);
		}
	}
}

void NetworkThread::Run(){
//	double t=Now();
	while(running){
		//receive
		TaskHeader hdr;
		if(net->probe(&hdr)){
			string data=net->receive(&hdr);
//			DLOG_IF(INFO,hdr.type!=4)<<"Receive(t) from "<<hdr.src_dst<<" to "<<id()<<", type "<<hdr.type;
			stats["received bytes"] += hdr.nBytes;
			stats["received type."+to_string(hdr.type)] += 1;
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
			lock_guard<recursive_mutex> sl(ps_lock);
//			DLOG_IF(INFO,Now()-t>12.)<<id()<<" pending # : "<<pending_sends_.size();
			for(auto it=pending_sends_.begin();it!=pending_sends_.end();++it)
				net->send(*it);
			pending_sends_.clear();
		}
		/* single send: */
//		while(!pending_sends_.empty()){
//			//boost::recursive_mutex::scoped_lock sl(ps_lock);
//			lock_guard<recursive_mutex> sl(ps_lock);
//			Task* s = pending_sends_.back();
//			pending_sends_.pop_back();
////		sl.~lock_guard();
//			net->send(s);
//		}
//		DLOG_IF(INFO,Now()-t>12. && tt.elapsed()>0.001)<<"send time on "<<id()<<" is "<<tt.elapsed();
//		DLOG_IF(INFO,Now()-t>12. && !pending_sends_.empty())<<"on "<<id()<<" ps len="<<pending_sends_.size()<<", us len="<<net->unconfirmedTaskNum();

//		PERIODIC(10., {
//			DumpProfile()
//			;
//		});
	}
}

bool NetworkThread::CheckQueue(NetworkThread::Queue& q, recursive_mutex& m, Message* data){
	if(!q.empty()){
		lock_guard<recursive_mutex> sl(m);
		if(q.empty()) return false;

		if(data){
			const string& s = q.front();
			data->ParseFromArray(s.data() + sizeof(MsgHeader), s.size() - sizeof(MsgHeader));
		}

		q.pop_front();
		return true;
	}
	return false;
}

bool NetworkThread::checkReceiveQueue(int src, int type, Message* data){
	CHECK_LT(src, kMaxHosts);
	CHECK_LT(type, kMaxMethods);

	Queue& q = receive_buffer[type][src];
	VLOG_IF(2, q.size() != 0 && q.size() % 10 == 0)
			<< "RECEIVE QUEUE SIZE for type " << type << " src " << src << " is " << q.size();
	return CheckQueue(q,rec_lock[type],data);
}

bool NetworkThread::checkReplyQueue(int src, int type, Message* data){
	CHECK_LT(src, kMaxHosts);
	CHECK_LT(type, kMaxMethods);

	Queue& q = reply_buffer[type][src];
	VLOG_IF(2, q.size() != 0 && q.size() % 10 == 0)
			<< "REPLY QUEUE SIZE for type " << type << " src " << src << " is " << q.size();
	return CheckQueue(q,rep_lock[type],data);
}

// Blocking read for the given source and message type.
void NetworkThread::Read(int desired_src, int type, Message* data, int *source){
	Timer t;
	while(!TryRead(desired_src, type, data, source)){
		Sleep();
	}
	stats["network_time"] += t.elapsed();
}

bool NetworkThread::TryRead(int src, int type, Message* data, int *source){
	if(src != TaskBase::ANY_SRC){
		if(checkReceiveQueue(src, type, data)){
			if(source)
				*source = src;
			return true;
		}
	}else{//any source
		for(int i = 0; i < size(); ++i){
			if(TryRead(i, type, data, source))
				return true;
		}
	}
	return false;
}

void NetworkThread::Call(int dst, int method, const Message &msg, Message *reply){
	Send(dst, method, msg);
//	Timer t;
	while(!checkReplyQueue(dst, method, reply)){
		Sleep();
	}
}

// Enqueue the given request to pending buffer for transmission.
inline int NetworkThread::Send(Task *req){
//	DLOG_IF(INFO,req->type!=4)<<"Sending(t) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type;
	int size=req->payload.size();
	stats["sent bytes"] += size;
	stats["sent type."+to_string(req->type)] += 1;
	Timer t;
	lock_guard<recursive_mutex> sl(ps_lock);
//	DLOG_IF(INFO,t.elapsed()>0.005)<<"Sending(l) from "<<id()<<" to "<<req->src_dst<<", type "<<req->type<<", lock time="<<t.elapsed();
	pending_sends_.push_back(req);
	return size;
}
int NetworkThread::Send(int dst, int method, const Message &msg){
	return Send(new Task(dst,method,msg,MsgHeader()));
}

// Directly (Physically) send the request.
inline int NetworkThread::DSend(Task *req){
	int size=req->payload.size();
	net->send(req);
	return size;
}
int NetworkThread::DSend(int dst, int method, const Message &msg){
	return DSend(new Task(dst,method,msg));
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
	int myid=id();
	for(int i = 0; i < net->size(); ++i){
		if(i!=myid)
			Send(i, method, msg);
	}
}

void NetworkThread::SyncBroadcast(int method, const Message& msg){
	VLOG(2) << "Sending: " << msg.ShortDebugString();
	Broadcast(method, msg);
	WaitForSync(method, net->size() - 1);
}

void NetworkThread::WaitForSync(int method, int count){
	vector<bool> replied(size(),false);
	while(count > 0){
		for(int i = 0; i < net->size(); ++i){
			if(replied[i]==false && checkReplyQueue(i, method, nullptr)){
				--count;
				replied[i]=true;
			}
		}
		Sleep();
	}
}

void NetworkThread::_RegisterCallback(int message_type, Message *req, Message* resp, Callback cb){
	CallbackInfo *cbinfo = new CallbackInfo;

	cbinfo->spawn_thread = false;
	cbinfo->req = req;
	cbinfo->resp = resp;
	cbinfo->call = cb;

	CHECK_LT(message_type, kMaxMethods)<< "Message type: " << message_type << " over limit.";
	callbacks_[message_type] = cbinfo;
}

void NetworkThread::SpawnThreadFor(int req_type){
	callbacks_[req_type]->spawn_thread = true;
}

static NetworkThread* self = NULL;
NetworkThread* NetworkThread::Get(){
	return self;
}

static void ShutdownMPI(){
	NetworkThread::Get()->Shutdown();
}

void NetworkThread::Init(){
	VLOG(1) << "Initializing network...";
	CHECK(self == NULL);
	self = new NetworkThread();
	atexit(&ShutdownMPI);
}

string NetworkThread::receiveQueueOccupation(){
	string s;
	for(int i=0;i<kMaxMethods;++i){
		lock_guard<recursive_mutex> sl(rec_lock[i]);
		for(int j=0;j<kMaxHosts;++j)
			if(!receive_buffer[i][j].empty())
				s+=to_string(i)+"-"+to_string(j)+":"+to_string(receive_buffer[i][j].size())+"\n";
	}
	return s;
}

}

