#include "client/client.h"
#include "net/NetworkThread.h"
#include "examples/examples.h"
#include <glog/logging.h>
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

//DEFINE_int32(bufmsg, 10000, "expected minimum number of message per sending");
DEFINE_double(bufmsg_portion, 0.01,"portion of buffered sending");
DEFINE_double(buftime, 0.5, "maximum time interval between 2 sendings");
DEFINE_int32(snapshot_interval, 10, "termination check interval (second)");

DEFINE_string(graph_dir, "graph", "graph files");
DEFINE_string(result_dir, "result", "result files");
DEFINE_int32(max_iterations, 100, "");
DEFINE_int64(num_nodes, 100, "hint for number of nodes");
DEFINE_double(portion, 1, "");
DEFINE_double(termcheck_threshold, 1000000000, "");
DEFINE_double(sleep_time, 0.001, "");

DEFINE_string(checkpoint_dir, "", "The directory of writing/loading checkpoints");
DEFINE_string(checkpoint_type, "CP_NONE", "Type of checkpoint mechanism");
DEFINE_double(checkpoint_interval, 0.0, "Interval of taking checkpoint (in second)");
DEFINE_int32(checkpoint_restore_from, -1, "The epoch to restore from (-1 for do not restore).");

DEFINE_double(flush_time, 0.2, "waiting time for flushing out all network message");

DEFINE_string(hostfile, "conf/maiter-cluster", "");
//DEFINE_int32(workers, 2, "");

//DEFINE_bool(localtest, false, "");
DEFINE_bool(run_tests, false, "");


void Init(int argc, char** argv){
	FLAGS_logtostderr = true;
	FLAGS_logbuflevel = -1;

	//CHECK_EQ(lzo_init(), 0);

	google::SetUsageMessage("invoke from mpirun, using --runner to select control function.");
	google::ParseCommandLineFlags(&argc, &argv, false);
	google::InitGoogleLogging(argv[0]);
	google::InstallFailureSignalHandler();

#ifdef CPUPROF
	if(FLAGS_cpu_profile){
		mkdir("profile/", 0755);
		char buf[100];
		gethostname(buf, 100);
		ProfilerStart(StringPrintf("profile/cpu.%s.%d", buf, getpid()).c_str());
	}
#endif

#ifdef HEAPPROF
	char buf[100];
	gethostname(buf, 100);
	HeapProfilerStart(StringPrintf("profile/heap.%s.%d", buf, getpid()).c_str());
#endif

	RunInitializers();

	if(FLAGS_run_tests){
		RunTests();
		exit(0);
	}

	RegisterExamples();

	ios_base::sync_with_stdio(false);

	NetworkThread::Init(argc, argv);

	// If we are not running in the context of MPI, go ahead and invoke
	// mpirun to start ourselves up.
	/*
	if(!getenv("OMPI_UNIVERSE_SIZE")){
		string cmd = StringPrintf("mpirun "
			" -hostfile \"%s\""
			" -bycore"
			" -nooversubscribe"
			" -n %d"
			" %s"
			" --log_prefix=true ",
			FLAGS_hostfile.c_str(),
			FLAGS_workers,
			JoinString(&argv[0], &argv[argc]).c_str()
		);

		LOG(INFO) << "Invoking MPI..." << cmd;
		system(cmd.c_str());
		exit(0);
	}
	*/

	//srandom(time(NULL));
}

int main(int argc, char** argv){
	FLAGS_log_prefix = false;

	Init(argc, argv);

	//CHECK(NetworkThread::Get()->size() > 1) << "Number of MPI instances should be more than 1";

	ConfigData conf;
	conf.set_num_workers(NetworkThread::Get()->size() - 1);
	conf.set_worker_id(NetworkThread::Get()->id() - 1);

#ifndef NDEBUG
	for(auto& p : RunnerRegistry::Get()->runners()){
		LOG(INFO) << p.first << " -> " << p.second;
	}
#endif

//  LOG(INFO) << "Running: " << FLAGS_runner;
	CHECK_NE(FLAGS_runner, "");
	RunnerRegistry::KernelRunner k = RunnerRegistry::Get()->runner(FLAGS_runner);
	LOG(INFO)<< "kernel runner is " << FLAGS_runner;
	CHECK(k != NULL) << "Could not find kernel runner " << FLAGS_runner;
	k(conf);
	LOG(INFO)<< "Exiting.";
	NetworkThread::Shutdown();
	LOG(INFO) << "Done.";
}
