/*
 * NetworkKernel.h
 *
 *  Created on: Nov 29, 2015
 *      Author: tzhou
 */

#ifndef NET_NETWORKIMPLMPI_H_
#define NET_NETWORKIMPLMPI_H_

#include <deque>
#include <string>
#include <vector>
#include <mpi.h>
#include <mutex>
#include "Task.h"

namespace dsm {

/*
 * Code related with underlying implementation (MPI used here)
 */
class NetworkImplMPI{
public:
	// Probe is there any message, return whether find one, if any the message info in hdr.
	bool probe(TaskHeader* hdr);
	// Receive a message with given header.
	std::string receive(const TaskHeader* hdr);
	std::string receive(int dst, int type, const int nBytes);
	// Try send out a message with given hdr and content.
	void send(const Task* t);
//	void send(const int dst, const int type, const std::string& data);
//	void send(const int dst, const int type, std::string&& data);

	static NetworkImplMPI* GetInstance();
	static int TransformSrc(int s_d){
		return s_d==TaskBase::ANY_SRC ? MPI::ANY_SOURCE : s_d;
	}
	static int TransformTag(int tag){
		return tag==TaskBase::ANY_TYPE ? MPI::ANY_TAG : tag;
	}

	int id() const;
	int size() const;
	void shutdown();

	// Check unfinished send buffer and remove those have succeeded, return left task number.
	size_t collectFinishedSend();
	// Get info of the unfinished sending tasks (do not update it)
	size_t unconfirmedTaskNum() const;
	size_t unconfirmedBytes() const;
	std::vector<const Task*> unconfirmedTask() const;
private:
	NetworkImplMPI();
private:
	MPI::Intracomm world;
	int id_;
	int size_;

	struct TaskSendMPI{
		const Task* tsk;
		MPI::Request req;
	};

	std::deque<TaskSendMPI> unconfirmed_send_buffer;
	mutable std::recursive_mutex us_lock;
};

inline int NetworkImplMPI::id() const{
	return id_;
}
inline int NetworkImplMPI::size() const{
	return size_;
}


} /* namespace dsm */

#endif /* NET_NETWORKIMPLMPI_H_ */
