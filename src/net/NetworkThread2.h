#ifndef NET_NETWORKTHREAD2_H_
#define NET_NETWORKTHREAD2_H_

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
class NetworkThread2{
public:
	bool active() const;
	int64_t pending_bytes() const;
	int waiting_messages() const;

	// Blocking read for the given source and message type.
	void ReadAny(string& data, int *sourcsrcRete=nullptr, int *typeRet=nullptr);
	bool TryReadAny(string& data, int *sosrcReturce=nullptr, int *typeRet=nullptr);

	// Enqueue the given request to pending buffer for transmission.
//  void Send(RPCRequest *req);
	int Send(int dst, int tag, const Message &msg);
	// Directly send the request bypassing the pending buffer.
	int DSend(int dst, int method, const Message &msg);
//  void ObjectCreate(int dst, int method);

	void Broadcast(int method, const Message& msg);
	void SyncBroadcast(int method, const Message& msg);
	void WaitForSync(int method, int count);

	void Flush();
	void Shutdown();

	int id() const;
	int size() const;

	static NetworkThread2 *Get();
	static void Init();

	Stats stats;
	// Register the given function with the RPC thread.  The function will be invoked
	// from within the network thread whenever a message of the given type is received.
	typedef std::function<void(const std::string&, const RPCInfo& rpc)> Callback;

	struct CallbackInfo{
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
	std::deque<std::pair<std::string,TaskBase> > receive_buffer;
	mutable std::recursive_mutex rec_lock;
//	Queue receive_buffer[kMaxMethods][kMaxHosts];
//	mutable std::recursive_mutex rec_lock[kMaxMethods];
	Queue reply_buffer[kMaxMethods][kMaxHosts];
	mutable std::recursive_mutex rep_lock[kMaxMethods];

	// Enqueue the given request to pending buffer for transmission.
	int Send(Task *req);
	// Directly (Physically) send the request.
	int DSend(Task *req);

	bool checkReceiveQueue(std::string& data, TaskBase& info);

	void ProcessReceivedMsg(int source, int tag, std::string& data);
	void Run();

	NetworkThread2();
};

}

#endif // NET_NETWORKTHREAD_H_
