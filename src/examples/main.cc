#include "client/client.h"
#include "net/NetworkThread.h"
#include <iostream>

using namespace dsm;
using namespace std;

DEFINE_string(runner, "", "");
DEFINE_int32(taskid,0,"unique id for distinguishing different task");

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");
DEFINE_int32(block_size, 10, "");
DEFINE_int32(edge_size, 1000, "");
DEFINE_bool(build_graph, false, "");
DEFINE_bool(dump_results, false, "");

DEFINE_bool(local_aggregate, true, "whether to perform local aggregation");

//DEFINE_int32(bufmsg, 10000, "expected minimum number of message per sending");
DEFINE_double(bufmsg_portion, 0.01,"portion of buffered sending");
DEFINE_double(buftime, 3.0, "maximum time interval between 2 sendings");

DEFINE_double(snapshot_interval, 20, "termination checking interval, in seconds");

DEFINE_string(graph_dir, "subgraphs", "");
DEFINE_string(result_dir, "result", "");
DEFINE_string(init_dir, "", "the folder used to load initial values of each node");
DEFINE_string(delta_prefix, "", "the prefix (path and name prefix) for delta graphs. <-0>, <-1>, ... are added for each part");

DEFINE_int32(max_iterations, 100, "");
DEFINE_int64(num_nodes, 100, "");
DEFINE_double(portion, 1, "");
DEFINE_double(termcheck_threshold, 1000000000, "");
DEFINE_double(sleep_time, 0.001, "");

DEFINE_int64(graph_source, 0, "the source node for some graph algorithms like shortest-path, widest-path");
DEFINE_double(weight_alpha, 1, "the factor for the bad news");
DEFINE_bool(priority_degree, false, "whether to multiple the degree to the priority");

int main(int argc, char** argv){
	FLAGS_log_prefix = false;
//  cout<<getcallstack()<<endl;

	Init(argc, argv);

	CHECK_NE(FLAGS_runner, "");

	ConfigData conf;
	conf.set_num_workers(NetworkThread::Get()->size() - 1);
	conf.set_worker_id(NetworkThread::Get()->id() - 1);

	RunnerRegistry::KernelRunner k = RunnerRegistry::Get()->runner(FLAGS_runner);
	if(NetworkThread::Get()->id() == 0){
		LOG(INFO) << "Kernel runner is " << FLAGS_runner;
		LOG(INFO) << "Number of shards: " << conf.num_workers();
	}
	CHECK(k != NULL) << "Could not find kernel runner " << FLAGS_runner;
	k(conf);
	LOG(INFO)<< "Exiting.";
}
