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
#include <fstream>

using namespace std;

DECLARE_string(net_ratio);
DECLARE_string(bandwidth_folder);
DECLARE_int32(bandwidth_window);

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
	if(!parseRatio()) {
		LOG(FATAL) << "Cannot parse sending ratio!";
	}
	DVLOG(1)<<"ratio="<<ratio;
	MPI::Init_thread(MPI_THREAD_SINGLE);

	MPI_Errhandler handler;
	MPI_Errhandler_create(&CrashOnMPIError, &handler);
	MPI::COMM_WORLD.Set_errhandler(handler);

	world = MPI::COMM_WORLD;
	id_ = world.Get_rank();
	size_=world.Get_size();

	// ratio control variables
	for(size_t i=0;i<NET_NUM_LAST;++i)
		net_last.push(ratio);
	net_sum=NET_NUM_LAST*ratio; // assumed default delay 1ms

	measuring=false;
	BANDWIDTH_WINDOW = FLAGS_bandwidth_window;
}

bool NetworkImplMPI::parseRatio()
{
	string s = FLAGS_net_ratio;
	for(size_t i = 0; i < s.size(); ++i) {
		if(s[i] >= 'A' && s[i] <= 'Z')
			s[i] += 'a' - 'A';
	}
	bool flag = true;
	size_t scale = 1;
	if(s.size() < 2) {
		flag = false;
	} else if(s == "inf") {
		ratio = numeric_limits<decltype(ratio)>::max();
	} else{
		if(s.back()=='k' || s.back()=='m' || s.back()=='g') {
			if(s.back() == 'k')
				scale = 1000;
			else if(s.back() == 'm')
				scale = 1000 * 1000;
			else
				scale = 1000 * 1000 * 1000;
			s = s.substr(0, s.size() - 1);
		}
		try {
			ratio = stod(s);
		} catch(...) {
			flag = false;
		}
	}
	if(flag && scale != 1) {
		ratio *= scale;
	}
	return flag;
}

void NetworkImplMPI::start_measure_bandwidth_usage(){
	measuring=!FLAGS_bandwidth_folder.empty();
	measure_start_time=Now();
	//bandwidth_usage.clear();
}
void NetworkImplMPI::stop_measure_bandwidth_usage(){
	measuring=false;
}
void NetworkImplMPI::update_bandwidth_usage(const size_t size, double t_b, double t_e){
	t_b-=measure_start_time;
	t_e-=measure_start_time;
	int idx_b=static_cast<int>(t_b)/BANDWIDTH_WINDOW;
	int idx_e=static_cast<int>(t_e)/BANDWIDTH_WINDOW;
	// expend
	if(bandwidth_usage.size() <= idx_e){
		if(bandwidth_usage.capacity() <= idx_e)
			bandwidth_usage.reserve(idx_e*2);
		bandwidth_usage.resize(idx_e+1, 0.0);
	}
	// update bandwidth usage
	if(idx_b==idx_e){
		bandwidth_usage[idx_b]+=size;
	}else{
		double t=t_e-t_b;
		double r=size/t/BANDWIDTH_WINDOW;
		bandwidth_usage[idx_b] += r*((idx_b+1)*BANDWIDTH_WINDOW - t_b);
		bandwidth_usage[idx_e] += r*(t_e - idx_e*BANDWIDTH_WINDOW);
		for(int i=idx_b+1; i<idx_e; ++i){
			bandwidth_usage[i]+=r;
		}
	}
}
void NetworkImplMPI::dump_bandwidth_usage(){
	if(id()!=0 && !FLAGS_bandwidth_folder.empty()){
		ofstream fout(FLAGS_bandwidth_folder+"/band-"+to_string(id()-1));
		for(auto v : bandwidth_usage)
			fout<<v<<" ";
	}
}

NetworkImplMPI* self=nullptr;
NetworkImplMPI* NetworkImplMPI::GetInstance(){
	if(self==nullptr)
		self=new NetworkImplMPI();
	return self;
}

void NetworkImplMPI::Shutdown(){
	if(!MPI::Is_finalized()){
		VLOG(1)<<"Shut down MPI at rank "<<MPI::COMM_WORLD.Get_rank();
		MPI::Finalize();
	}
	self->dump_bandwidth_usage();
	delete self;
	self=nullptr;
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
//	VLOG_IF(2,hdr->type!=4)<<"Receive(m) from "<<hdr->src_dst<<" to "<<id()<<", type "<<hdr->type;
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
//	VLOG_IF(2,t->type!=4)<<"Sending(m) from "<<id()<<" to "<<t->src_dst<<", type "<<t->type;
	lock_guard<recursive_mutex> sl(us_lock);
	Timer tmr;
	double t_limit=t->payload.size()/ratio;
	double t_estimated_trans=t->payload.size()/(net_sum/net_last.size());
	TaskSendMPI tm{t,
		world.Isend(t->payload.data(), t->payload.size(), MPI::BYTE,t->src_dst, t->type),
		Now()
	};
	unconfirmed_send_buffer.push_back(tm);
	double t_control=t_limit-t_estimated_trans-tmr.elapsed();
//	LOG_EVERY_N(INFO, 100)<<t_control;
	if(t_control>0.0)
		Sleep(t_control);
}
//void NetworkImplMPI::send(const int dst, const int type, const std::string& data){
//	send(new Task(dst,type,data));
//}
//void NetworkImplMPI::send(const int dst, const int type, std::string&& data){
//	send(new Task(dst,type,move(data)));
//}

void NetworkImplMPI::broadcast(const Task* t){
	//MPI::IBcast does not support tag
	int myid = id();
	for(int i = 0; i < size(); ++i){
		if(i != myid){
			//make sure each pointer given to send() is unique
			Task* t2=new Task(*t);
			t2->src_dst=i;
			send(t2);
		}
	}
	delete t;
}

////
// State checking
////
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
//			VLOG_IF(2,it->tsk->type!=4)<< "Sending(f) from "<<id()<<" to " << it->tsk->src_dst<< " of type " << it->tsk->type;
			size_t size=it->tsk->payload.size();
			double now=Now();
			if(size>=NET_MINIMUM_LEN){
				double v=size/(now-it->stime);
				net_last.push(v);
				net_sum+=v-net_last.front();
				net_last.pop();
//				LOG_EVERY_N(INFO,100)<<"ratio updated to "<<net_sum/net_last.size();
			}
			if(measuring)
				update_bandwidth_usage(size, it->stime, now);
			delete it->tsk;
			it=unconfirmed_send_buffer.erase(it);
		}else
			++it;
	}
	return unconfirmed_send_buffer.size();
}
//size_t NetworkImplMPI::unconfirmedTaskNum() const{
//	return unconfirmed_send_buffer.size();
//}
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
