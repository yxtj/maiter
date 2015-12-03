/*
 * NetworkImplMPI.cpp
 *
 *  Created on: Nov 29, 2015
 *      Author: tzhou
 */

#include "NetworkImplMPI.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <mpi.h>

using namespace std;

namespace dsm {

static void CrashOnMPIError(MPI_Comm * c, int * errorCode, ...){
//	static dsm::SpinLock l;
//	l.lock();

	char buffer[1024];
	int size = 1024;
	MPI_Error_string(*errorCode, buffer, &size);
	LOG(FATAL)<< "MPI function failed: " << buffer;
}

NetworkImplMPI::NetworkImplMPI(): world(nullptr),id_(-1),size_(0){
	if(!getenv("OMPI_COMM_WORLD_RANK")){
		LOG(FATAL)<< "OpenMPI is not running!";
	}
	MPI::Init_thread(MPI_THREAD_SINGLE);

	MPI_Errhandler handler;
	MPI_Errhandler_create(&CrashOnMPIError, &handler);
	MPI::COMM_WORLD.Set_errhandler(handler);

	world = MPI::COMM_WORLD;
	id_ = world.Get_rank();
	size_=world.Get_size();
}

NetworkImplMPI* self=nullptr;
NetworkImplMPI* NetworkImplMPI::GetInstance(){
	if(self==nullptr)
		self=new NetworkImplMPI();
	return self;
}

void NetworkImplMPI::shutdown(){
	VLOG(2)<<"Shut down MPI at rank "<<id();
	MPI::Finalize();
}

////
// Transmitting functions:
////

bool NetworkImplMPI::probe(TaskHeader* hdr){
	MPI::Status st;
	if(!world.Iprobe(MPI::ANY_SOURCE, MPI::ANY_TAG, st))
		return false;
	hdr->src_dst=st.Get_source();
	hdr->type=st.Get_tag();
	hdr->nBytes=st.Get_count(MPI::BYTE);
	return true;
}
std::string NetworkImplMPI::receive(const TaskHeader* hdr){
//	DLOG_IF(INFO,hdr->type!=4)<<"Receive(m) from "<<hdr->src_dst<<" to "<<id()<<", type "<<hdr->type;
	string data(hdr->nBytes,'\0');
	world.Recv(const_cast<char*>(data.data()), hdr->nBytes, MPI::BYTE, hdr->src_dst, hdr->type);
	return data;
}
std::string NetworkImplMPI::receive(int dst, int type, const int nBytes){
	string data(nBytes,'\0');
	// address transfer
	dst=TransformSrc(dst);
	type=TransformTag(type);
	world.Recv(const_cast<char*>(data.data()), nBytes, MPI::BYTE, dst, type);
	return data;
}

void NetworkImplMPI::send(const Task* t){
//	DLOG_IF(INFO,t->type!=4)<<"Sending(m) from "<<id()<<" to "<<t->src_dst<<", type "<<t->type;
	lock_guard<recursive_mutex> sl(us_lock);
	TaskSendMPI tm{t,
		world.Isend(t->payload.data(), t->payload.size(), MPI::BYTE,t->src_dst, t->type)};
	unconfirmed_send_buffer.push_back(tm);
}
//void NetworkImplMPI::send(const int dst, const int type, const std::string& data){
//	send(new Task(dst,type,data));
//}
//void NetworkImplMPI::send(const int dst, const int type, std::string&& data){
//	send(new Task(dst,type,move(data)));
//}

////
// State checking
////
double t=Now();
size_t NetworkImplMPI::collectFinishedSend(){
	if(unconfirmed_send_buffer.empty())
		return 0;
	//XXX: this lock may lower down performance significantly
	lock_guard<recursive_mutex> sl(us_lock);
//	VLOG(3) << "Unconfirmed sends #: " << unconfirmed_send_buffer.size();
	deque<TaskSendMPI>::iterator it=unconfirmed_send_buffer.begin();
//	DLOG_IF(INFO,Now()-t>12.)<<id()<<" Unconfirmed sends #: " << unconfirmed_send_buffer.size()<<". Top one: to "<<it->tsk->src_dst<<",type "<<it->tsk->type;
	while(it!=unconfirmed_send_buffer.end()){
//		VLOG(3) << "Unconfirmed at " << id()<<": "<<it->tsk->src_dst<<" , "<<it->tsk->type;
		if(it->req.Test()){
//			VLOG(3) << "Finished send from "<<id()<<" to " << it->tsk->src_dst<< " of size " << it->tsk->payload.size();
			delete it->tsk;
			it=unconfirmed_send_buffer.erase(it);
		}else
			++it;
	}
	return unconfirmed_send_buffer.size();
}
size_t NetworkImplMPI::unconfirmedTaskNum() const{
	return unconfirmed_send_buffer.size();
}
std::vector<const Task*> NetworkImplMPI::unconfirmedTask() const{
	lock_guard<recursive_mutex> sl(us_lock);
	std::vector<const Task*> res;
	res.reserve(unconfirmed_send_buffer.size());
	for(const TaskSendMPI& ts : unconfirmed_send_buffer){
		res.push_back(ts.tsk);
	}
	return res;
}
size_t NetworkImplMPI::unconfirmedBytes() const{
	lock_guard<recursive_mutex> sl(us_lock);
	size_t res=0;
	for(const TaskSendMPI& ts : unconfirmed_send_buffer){
		res+=ts.tsk->payload.size();
	}
	return res;
}

} /* namespace dsm */
