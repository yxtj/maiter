/*
 * MsgDriver.h
 *
 *  Created on: Dec 15, 2015
 *      Author: tzhou
 */

#ifndef DRIVER_MSGDRIVER_H_
#define DRIVER_MSGDRIVER_H_

#include "Dispatcher.hpp"
#include "net/RPCInfo.h"
//#include <google/protobuf/message.h>
#include <deque>
#include <string>
//#include <thread>
#include <functional>

namespace dsm {

class NetworkThread;

class MsgDriver{
public:
//	typedef google::protobuf::Message Message;
	typedef Dispatcher<const std::string&, const RPCInfo&>::callback_t callback_t;
	static callback_t GetDummyHandler();

	MsgDriver();
	// Launch the Message Driver. Data flow is as below:
	// data->immediateDispatcher--+-->queue->processDispatcher-+->end
	//                            +-->processed->end           +->defaultHandle->end
	void delegated_run();
	void terminate();
	//TODO: change run() to be a launcher of next 2 thread.
//	void inputThread();
//	void outputThread();

	// Link an inputer source to read from.
	void linkInputter(NetworkThread* input);
	// For message should be handled at receiving time (i.e. alive check)
	void registerImmediateHandler(const int type, callback_t cb, bool spawnThread=false);
	void unregisterImmediateHandler(const int type);
	// For message should be handled in sequence (i.e. data update)
	void registerProcessHandler(const int type, callback_t cb, bool spawnThread=false);
	void unregisterProcessHandler(const int type);

	void registerDefaultOutHandler(callback_t cb);

	void resetImmediateHandler();
	void resetProcessHandler();
	void resetDefaultOutHandler();
	void resetWaitingQueue();
	void clear();

	void readBlocked(std::string& data, RPCInfo& info);
	bool readUnblocked(std::string& data, RPCInfo& info);

	// return whether the input bypasses the dispatcher (enqueue)
	bool pushData(std::string& data, RPCInfo& info);
	// return whether a value is picked and bypasses the dispatcher (default handled)
	bool popData();

private:
	//return whether the provided data bypasses dispatcher (enqueue & default handled)
	bool processInput(std::string& data, RPCInfo& info);
	bool processOutput(std::string& data, RPCInfo& info);

	bool running_;
	NetworkThread *net;

	Dispatcher<const std::string&, const RPCInfo&> netDisper; //immediately response
	std::deque<std::pair<std::string, RPCInfo> > que; //queue for message waiting for process
	Dispatcher<const std::string&, const RPCInfo&> queDisper; //response when processed
	callback_t defaultHandler;
};

} /* namespace dsm */

#endif /* DRIVER_MSGDRIVER_H_ */
