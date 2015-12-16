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
#include <google/protobuf/message.h>
#include <queue>
#include <string>
//#include <thread>
#include <functional>

namespace dsm {

class NetworkThread;

class MsgDriver{
public:
	typedef google::protobuf::Message Message;
	typedef Dispatcher<const std::string&, const RPCInfo&>::callback_t cb_net_t;
	typedef Dispatcher<const std::string&>::callback_t cb_que_t;

	MsgDriver();
	void run();

	void registerNetDispFun(const int type, cb_net_t cb);
	void registerQueDispFun(const int type, cb_que_t cb);
	void registerDefaultHandler(cb_que_t cb);
	void linkInputter(NetworkThread* input);

	static bool DecodeMessage(const int type, const std::string& data, Message* msg);

	void readBlocked(std::string& data, RPCInfo& info);
	bool readUnblocked(std::string& data, RPCInfo& info);
	void handleInput(std::string& data, RPCInfo& info);
	void handleOutput(const std::string& data, const int type);

private:
	bool running_;
	Dispatcher<const std::string&, const RPCInfo&> netDisper; //immediately response
	std::queue<std::pair<int,std::string> > que; //queue for message waiting for process
	Dispatcher<const std::string&> queDisper; //response when processed
	cb_que_t defaultHandler;

	NetworkThread *net;
};

} /* namespace dsm */

#endif /* DRIVER_MSGDRIVER_H_ */
