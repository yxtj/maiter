#include "client/client.h"
#include "net/NetworkThread.h"
#include <iostream>

using namespace dsm;
using namespace std;

DEFINE_string(runner, "", "");
DEFINE_int32(taskid,0,"unique id for distinguishing different task");

DEFINE_string(net_ratio, "inf", "maximum sending ratio on one worker (bytes per seconds). "
	"supports: inf, K (1000), M (10^6), G (10^9)");
DEFINE_double(net_delay_time, 0.0, "delay time before commiting a received message");

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");
DEFINE_int32(block_size, 10, "");
DEFINE_int32(edge_size, 1000, "");
DEFINE_bool(build_graph, false, "");
DEFINE_bool(dump_results, false, "");

//DEFINE_int32(bufmsg, 10000, "expected minimum number of message per sending");
DEFINE_double(bufmsg_portion, 0.01,"portion of buffered sending");
DEFINE_double(buftime, 3.0, "maximum time interval between 2 sendings");

DEFINE_string(graph_dir, "subgraphs", "");
DEFINE_string(result_dir, "result", "");
DEFINE_int32(max_iterations, 100, "");
DEFINE_int64(num_nodes, 100, "");
DEFINE_double(portion, 1, "");
DEFINE_double(termcheck_threshold, 1000000000, "");
DEFINE_double(sleep_time, 0.001, "");


int main(int argc, char** argv){
	FLAGS_log_prefix = false;
//  cout<<getcallstack()<<endl;

	Init(argc, argv);

	ConfigData conf;
	conf.set_num_workers(NetworkThread::Get()->size() - 1);
	conf.set_worker_id(NetworkThread::Get()->id() - 1);

//  cout<<NetworkThread::Get()->id()<<":"<<getcallstack()<<endl;
// return 0;
//  LOG(INFO) << "Running: " << FLAGS_runner;
	CHECK_NE(FLAGS_runner, "");
	RunnerRegistry::KernelRunner k = RunnerRegistry::Get()->runner(FLAGS_runner);
	LOG(INFO)<< "kernel runner is " << FLAGS_runner;
	CHECK(k != NULL) << "Could not find kernel runner " << FLAGS_runner;
	k(conf);
	LOG(INFO)<< "Exiting.";
}
