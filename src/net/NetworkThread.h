#ifndef NET_NETWORKTHREAD_H_
#define NET_NETWORKTHREAD_H_

#include <thread>
#include <mutex>
#include <functional>
#include <string>
#include <deque>
#include <vector>
#include "util/common.h"
#include <google/protobuf/message.h>
//#include "NetworkImplMPI.h"
#include "Task.h"
#include "RPCInfo.h"

namespace dsm {

typedef google::protobuf::Message Message;

class NetworkImplMPI;

// Hackery to get around mpi's unhappiness with threads.  This thread
// simply polls MPI continuously for any kind of update and adds it to
// a local queue.
class NetworkThread{
public:
	bool active() const;
	int64_t pending_bytes() const;

	// Blocking read for the given source and message type.
	void Read(int desired_src, int type, Message* data, int *source = nullptr);
	bool TryRead(int desired_src, int type, Message* data, int *source = nullptr);

	// Enqueue the given request to pending buffer for transmission.
//  void Send(RPCRequest *req);
	int Send(int dst, int tag, const Message &msg);
	// Directly send the request bypassing the pending buffer.
	int DSend(int dst, int method, const Message &msg);
//  void ObjectCreate(int dst, int method);

	void Broadcast(int method, const Message& msg);
	void SyncBroadcast(int method, const Message& msg);
	void WaitForSync(int method, int count);

	// Invoke 'method' on the destination, and wait for a reply.
	void Call(int dst, int method, const Message &msg, Message *reply);

	void Flush();
	void Shutdown();

	int id() const;
	int size() const;

	static NetworkThread *Get();
	static void Init();

	Stats stats;
	// Register the given function with the RPC thread.  The function will be invoked
	// from within the network thread whenever a message of the given type is received.
	typedef std::function<void(const RPCInfo& rpc)> Callback;

	// Use RegisterCallback(...) instead.
	void _RegisterCallback(int req_type, Message *req, Message *resp, Callback cb);

	// After registering a callback, indicate that it should be invoked in a
	// separate thread from the RPC server.
	void SpawnThreadFor(int req_type);

	struct CallbackInfo{
		Message *req;
		Message *resp;
		Callback call;
		bool spawn_thread;
	};

//	static constexpr int ANY_SRC = TaskBase::ANY_SRC;
//	static constexpr int ANY_TAG = TaskBase::ANY_TYPE;

	// For debug purpose: get the length of receive buffer for all types and sources, display non-zero entries.
	// i.e.: 2-4:1 means there is 1 received and unprocessed message of type 2 from 4.
	std::string receiveQueueOccupation();

private:
	static const int kMaxHosts = 512;
	static const int kMaxMethods = 64;


	bool running;
	NetworkImplMPI* net;
	mutable std::thread t_;

	CallbackInfo* callbacks_[kMaxMethods];

	std::vector<Task*> pending_sends_;	//buffer for request to be sent
	mutable std::recursive_mutex ps_lock;

	typedef std::deque<std::string> Queue;
	Queue receive_buffer[kMaxMethods][kMaxHosts];
	mutable std::recursive_mutex rec_lock[kMaxMethods];
	Queue reply_buffer[kMaxMethods][kMaxHosts];
	mutable std::recursive_mutex rep_lock[kMaxMethods];

	// Enqueue the given request to pending buffer for transmission.
	int Send(Task *req);
	// Directly (Physically) send the request.
	int DSend(Task *req);

	bool checkReplyQueue(int src, int type, Message *data);
	bool checkReceiveQueue(int src, int type, Message* data);
	static bool CheckQueue(Queue& q, std::recursive_mutex& m, Message* data);

	void ProcessReceivedMsg(int source, int tag, std::string& data);
	void InvokeCallback(CallbackInfo *ci, RPCInfo rpc);
	void CollectFinishedSends();
	void Run();

	NetworkThread();
};

template<class Request, class Response, class Function, class Klass>
void RegisterCallback(int req_type, Request *req, Response *resp, Function function, Klass klass){
	NetworkThread::Get()->_RegisterCallback(req_type, req, resp,
			std::bind(function, klass, std::cref(*req), resp, std::placeholders::_1));
}


}

#endif // NET_NETWORKTHREAD_H_
