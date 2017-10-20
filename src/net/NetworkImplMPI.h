/*
 * NetworkKernel.h
 *
 *  Created on: Nov 29, 2015
 *      Author: tzhou
 */

#ifndef NET_NETWORKIMPLMPI_H_
#define NET_NETWORKIMPLMPI_H_

#include <string>
#include <vector>
#include <mutex>
#include <deque>
#include <queue>
#include <mpi.h>
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
	void broadcast(const Task* t);

	static NetworkImplMPI* GetInstance();
	static void Shutdown();
	static int TransformSrc(int s_d){
		return s_d==TaskBase::ANY_SRC ? MPI::ANY_SOURCE : s_d;
	}
	static int TransformTag(int tag){
		return tag==TaskBase::ANY_TYPE ? MPI::ANY_TAG : tag;
	}

	void start_measure_bandwidth_usage();
	void stop_measure_bandwidth_usage();

	int id() const;
	int size() const;

	// Check unfinished send buffer and remove those have succeeded, return left task number.
	size_t collectFinishedSend();
	// Get info of the unfinished sending tasks (do not update it)
	size_t unconfirmedTaskNum() const{
		return unconfirmed_send_buffer.size();
	}
	size_t unconfirmedBytes() const;
	std::vector<const Task*> unconfirmedTask() const;
private:
	NetworkImplMPI();
	bool parseRatio();
	void update_bandwidth_usage(const size_t size, double t_b, double t_e);
	void dump_bandwidth_usage();
private:
	MPI::Intracomm world;
	int id_;
	int size_;

	struct TaskSendMPI{
		const Task* tsk;
		MPI::Request req;
		double stime;
	};

	std::deque<TaskSendMPI> unconfirmed_send_buffer;
	mutable std::recursive_mutex us_lock;

	double ratio;

	std::queue<double> net_last; // transmission bandwidth of last N messages (>=M bytes)
	static constexpr size_t NET_MINIMUM_LEN=128;
	static constexpr size_t NET_NUM_LAST=8;
	double net_sum;

	bool measuring;
	double measure_start_time;
	std::vector<size_t> bandwidth_usage;
	size_t BANDWIDTH_WINDOW;
};

inline int NetworkImplMPI::id() const{
	return id_;
}
inline int NetworkImplMPI::size() const{
	return size_;
}


} /* namespace dsm */

#endif /* NET_NETWORKIMPLMPI_H_ */
