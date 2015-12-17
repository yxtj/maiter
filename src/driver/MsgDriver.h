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
	void run();
	void terminate();
	//TODO: change run() to be a launcher of next 2 thread.
//	void inputThread();
//	void outputThread();

	// Link an inputer source to read from.
	void linkInputter(NetworkThread* input);
	// For message should be handled at receiving time (i.e. alive check)
	void registerImmediateHandler(const int type, callback_t cb);
	void unregisterImmediateHandler(const int type);
	// For message should be handled in sequence (i.e. data update)
	void registerProcessHandler(const int type, callback_t cb);
	void unregisterProcessHandler(const int type);

	void registerDefaultOutHandler(callback_t cb);

	void resetImmediateHandler();
	void resetProcessHandler();
	void resetDefaultOutHandler();
	void resetWaitingQueue();
	void clear();

	void readBlocked(std::string& data, RPCInfo& info);
	bool readUnblocked(std::string& data, RPCInfo& info);
	void processInput(std::string& data, RPCInfo& info);
	void processOutput(const std::string& data, const RPCInfo& info);

private:
	bool running_;
	NetworkThread *net;

	Dispatcher<const std::string&, const RPCInfo&> netDisper; //immediately response
	std::deque<std::pair<std::string, RPCInfo> > que; //queue for message waiting for process
	Dispatcher<const std::string&, const RPCInfo&> queDisper; //response when processed
	callback_t defaultHandler;
};

} /* namespace dsm */

#endif /* DRIVER_MSGDRIVER_H_ */
