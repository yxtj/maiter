#include "util/rpc.h"
#include "util/common.h"
#include "util/common.pb.h"
#include <signal.h>

DECLARE_bool(localtest);
DECLARE_double(sleep_time);
DEFINE_bool(rpc_log, false, "");

namespace dsm {

static void CrashOnMPIError(MPI_Comm * c, int * errorCode, ...){
	static dsm::SpinLock l;
	l.lock();

	char buffer[1024];
	int size = 1024;
	MPI_Error_string(*errorCode, buffer, &size);
	LOG(FATAL)<< "MPI function failed: " << buffer;
}

struct Header{
	Header(bool is_reply_=false) :
			is_reply(is_reply_){}
	bool is_reply;
};

// Represents an active RPC to a remote peer.
struct RPCRequest: private boost::noncopyable{
	int target;
	int rpc_type;
	int failures;

	string payload;
	MPI::Request mpi_req;
	MPI::Status status;
	double start_time;

	RPCRequest(int target, int method, const Message& msg, Header h = Header());
	RPCRequest(int target, int method, Header h = Header());
	~RPCRequest();

	bool finished();
	double elapsed();
};

RPCRequest::~RPCRequest(){
}

bool RPCRequest::finished(){
	return mpi_req.Test(status);
}
double RPCRequest::elapsed(){
	return Now() - start_time;
}

// Send the given message type and data to this peer.
RPCRequest::RPCRequest(int tgt, int method, const Message& ureq, Header h) :
		target(tgt), rpc_type(method), failures(0), start_time(0.0){
	payload.append((char*)&h, sizeof(Header));
	ureq.AppendToString(&payload);
}

RPCRequest::RPCRequest(int tgt, int method, Header h) :
		target(tgt), rpc_type(method), failures(0), start_time(0.0){
	payload.append((char*)&h, sizeof(Header));
}

NetworkThread::NetworkThread(){
	if(!getenv("OMPI_COMM_WORLD_RANK")){
		world_ = NULL;
		id_ = -1;
		running = false;
		return;
	}

	MPI::Init_thread(MPI_THREAD_SINGLE);

	MPI_Errhandler handler;
	MPI_Errhandler_create(&CrashOnMPIError, &handler);
	MPI::COMM_WORLD.Set_errhandler(handler);

	world_ = &MPI::COMM_WORLD;
	running = 1;
	t_ = new boost::thread(&NetworkThread::Run, this);
	id_ = world_->Get_rank();

	for(int i = 0; i < kMaxMethods; ++i){
		callbacks_[i] = NULL;
	}
}

bool NetworkThread::active() const{
	return performed_sends_.size() + pending_sends_.size() > 0;
}

int NetworkThread::size() const{
	return world_->Get_size();
}

int64_t NetworkThread::pending_bytes() const{
	boost::recursive_mutex::scoped_lock sl(send_lock);
	int64_t t = 0;

	for(unordered_set<RPCRequest*>::const_iterator i = performed_sends_.begin();
			i != performed_sends_.end(); ++i){
		t += (*i)->payload.size();
	}

	for(int i = 0; i < pending_sends_.size(); ++i){
		t += pending_sends_[i]->payload.size();
	}

	return t;
}

/*
 * remove all the finished sending request from active sending buffer
 */
void NetworkThread::CollectFinishedSends(){
	if(performed_sends_.empty()) return;

	boost::recursive_mutex::scoped_lock sl(send_lock);
	VLOG(3) << "Pending sends: " << performed_sends_.size();
	unordered_set<RPCRequest*>::iterator i = performed_sends_.begin();
	while(i != performed_sends_.end()){
		RPCRequest *r = (*i);
		VLOG(3) << "Pending: " << MP(id(), MP(r->target, r->rpc_type));
		if(r->finished()){
			LOG_IF(INFO,r->failures>0) << "Send " << MP(id(), r->target) << " of size "
					<< r->payload.size() << " succeeded after " << r->failures << " failures.";
			VLOG(3) << "Finished send to " << r->target << " of size " << r->payload.size();
			delete r;
			i = performed_sends_.erase(i);
			continue;
		}
		++i;
	}
}

void NetworkThread::InvokeCallback(CallbackInfo *ci, RPCInfo rpc){
	ci->call(rpc);
	Header reply_header(true);
	Send(new RPCRequest(rpc.source, rpc.tag, *ci->resp, reply_header));
}
void NetworkThread::ProcessReceivedMsg(int source, int tag, string& data){
	//Case 1: received a reply packet, put into reply buffer
	//Case 2: received a RPC request, put into
	//Case 3: received a normal data packet, put into received buffer
	Header *h = (Header*)&data[0];
	if(h->is_reply){	//Case 1
		boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
		replies[tag][source].push_back(data);
	}else{
		if(callbacks_[tag] != NULL){	//Case 2
			CallbackInfo *ci = callbacks_[tag];
			ci->req->ParseFromArray(data.data() + sizeof(Header), data.size() - sizeof(Header));
			VLOG(2) << "Got incoming: " << ci->req->ShortDebugString();

			RPCInfo rpc = { source, id(), tag };
			if(ci->spawn_thread){
				boost::thread t(boost::bind(&NetworkThread::InvokeCallback, this, ci, rpc));
				t.detach();	//without it, the function is terminated when t is deconstructed.
			}else{
				InvokeCallback(ci, rpc);
			}
		}else{	//Case 3
			boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
			requests[tag][source].push_back(data);
		}
	}
}

void NetworkThread::Run(){
	while(running){
		MPI::Status st;

		//receive
		if(world_->Iprobe(MPI::ANY_SOURCE, MPI::ANY_TAG, st)){
			int tag = st.Get_tag();
			int source = st.Get_source();
			int bytes = st.Get_count(MPI::BYTE);

			string data;
			data.resize(bytes+1);
			world_->Recv(&data[0], bytes, MPI::BYTE, source, tag, st);

			stats["bytes_received"] += bytes;
			stats["received."+MessageTypes_Name((MessageTypes)tag)] += 1;
			CHECK_LT(source, kMaxHosts);

			VLOG(2) << "Received packet - source: " << source << " tag: " << tag;
			ProcessReceivedMsg(source, tag, data);
		}else{
			Sleep(FLAGS_sleep_time);
		}
		//send
		while(!pending_sends_.empty()){
			boost::recursive_mutex::scoped_lock sl(send_lock);
			RPCRequest* s = pending_sends_.back();
			pending_sends_.pop_back();
			DSend(s);
		}
		//manage send buffer
		CollectFinishedSends();

		PERIODIC(10., {
			DumpProfile()
			;
		});
	}
}

bool NetworkThread::check_request_queue(int src, int type, Message* data){
	CHECK_LT(src, kMaxHosts);
	CHECK_LT(type, kMaxMethods);

	Queue& q = requests[type][src];
	if(q.size() % 10 == 0 && q.size() != 0)
		VLOG(2) << "REQUEST QUEUE SIZE for type " << type << " src " << src << " is "<< q.size();
	if(!q.empty()){
		boost::recursive_mutex::scoped_lock sl(q_lock[type]);
		if(q.empty()) return false;

		const string& s = q.front();
		if(data){
			data->ParseFromArray(s.data() + sizeof(Header), s.size() - sizeof(Header));
		}

		q.pop_front();
		return true;
	}
	return false;
}

bool NetworkThread::check_reply_queue(int src, int type, Message* data){
	CHECK_LT(src, kMaxHosts);
	CHECK_LT(type, kMaxMethods);

	Queue& q = replies[type][src];
	if(q.size() % 10 == 0 && q.size() != 0)
		VLOG(1) << "REPLY QUEUE SIZE for type " << type << " src " << src << " is " << q.size();
	if(!q.empty()){
		boost::recursive_mutex::scoped_lock sl(q_lock[type]);
		if(q.empty()) return false;

		const string& s = q.front();
		if(data){
			data->ParseFromArray(s.data() + sizeof(Header), s.size() - sizeof(Header));
		}

		q.pop_front();
		return true;
	}
	return false;
}

// Blocking read for the given source and message type.
void NetworkThread::Read(int desired_src, int type, Message* data, int *source){
	Timer t;
	while(!TryRead(desired_src, type, data, source)){
		Sleep(FLAGS_sleep_time);
	}
	stats["network_time"] += t.elapsed();
}

bool NetworkThread::TryRead(int src, int type, Message* data, int *source){
	if(src != MPI::ANY_SOURCE){
		if(check_request_queue(src, type, data)){
			if(source)
				*source = src;
			return true;
		}
	}else{
		for(int i = 0; i < world_->Get_size(); ++i){
			if(TryRead(i, type, data, source))
				return true;
		}
	}
	return false;
}

void NetworkThread::Call(int dst, int method, const Message &msg, Message *reply){
	Send(dst, method, msg);
//	Timer t;
	while(!check_reply_queue(dst, method, reply)){
		Sleep(FLAGS_sleep_time);
	}
}

// Enqueue the given request to pending buffer for transmission.
inline int NetworkThread::Send(RPCRequest *req){
	boost::recursive_mutex::scoped_lock sl(send_lock);
//	LOG(INFO) << "Sending... " << MP(req->target, req->rpc_type);
//	LOG(INFO) << "Sending... from " << id() << " to " << req->target << " type: " << req->rpc_type << " size: " << req->payload.size();
	int size=req->payload.size();
	stats["bytes_sent"] += size;
	stats["sends."+MessageTypes_Name((MessageTypes)(req->rpc_type))] += 1;
	pending_sends_.push_back(req);
	return size;
}
int NetworkThread::Send(int dst, int method, const Message &msg){
	RPCRequest *r = new RPCRequest(dst, method, msg);
	return Send(r);
}

//void NetworkThread::ObjectCreate(int dst, int method){
//	RPCRequest *r = new RPCRequest(dst, method);
//	delete r;
//}

// Directly (Physically) send the request.
inline int NetworkThread::DSend(RPCRequest *req){
	boost::recursive_mutex::scoped_lock sl(send_lock);
	int size=req->payload.size();
	stats["bytes_sent"] += size;
	stats["sends."+MessageTypes_Name((MessageTypes)(req->rpc_type))] += 1;

	req->start_time = Now();
	req->mpi_req = world_->Isend(req->payload.data(), size, MPI::BYTE, req->target, req->rpc_type);
	performed_sends_.insert(req);
	return size;
}
int NetworkThread::DSend(int dst, int method, const Message &msg){
	RPCRequest *r = new RPCRequest(dst, method, msg);
	return DSend(r);
}

void NetworkThread::Shutdown(){
	if(running){
		Flush();
		running = false;
		MPI_Finalize();
	}
}

void NetworkThread::Flush(){
	while(active()){
		Sleep(FLAGS_sleep_time);
	}
}

void NetworkThread::Broadcast(int method, const Message& msg){
	for(int i = 1; i < world_->Get_size(); ++i){
		Send(i, method, msg);
	}
}

void NetworkThread::SyncBroadcast(int method, const Message& msg){
	VLOG(2) << "Sending: " << msg.ShortDebugString();
	Broadcast(method, msg);
	WaitForSync(method, world_->Get_size() - 1);
}

void NetworkThread::WaitForSync(int method, int count){
	EmptyMessage empty;
	while(count > 0){
		for(int i = 0; i < world_->Get_size(); ++i){
			if(check_reply_queue(i, method, NULL)) --count;
		}
		Sleep(FLAGS_sleep_time);
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

static NetworkThread* net = NULL;
NetworkThread* NetworkThread::Get(){
	return net;
}

static void ShutdownMPI(){
	NetworkThread::Get()->Shutdown();
}

void NetworkThread::Init(){
	VLOG(1) << "Initializing network...";
	CHECK(net == NULL);
	net = new NetworkThread();
	atexit(&ShutdownMPI);
}

}

