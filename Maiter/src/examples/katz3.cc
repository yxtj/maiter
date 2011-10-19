#include "client/client.h"


using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int32(num_nodes);
DECLARE_double(portion);
DECLARE_int32(max_iterations);
DECLARE_double(termcheck_threshold);
DECLARE_int32(katz_source);
DECLARE_double(katz_beta);


static TypedGlobalTable<int, float, float, vector<int> > *graph;
static map<int, float> priority_map;
static int iter = 0;
static Timer iter_timer;

static vector<int> readUnWeightLinks(string links){
    vector<int> linkvec;
    int spacepos = 0;
    while((spacepos = links.find_first_of(" ")) != links.npos){
        int to;
        if(spacepos > 0){
            to = boost::lexical_cast<int>(links.substr(0, spacepos));
        }
        links = links.substr(spacepos+1);
        linkvec.push_back(to);
    }

    return linkvec;
}

struct KatzScheduler : public Scheduler<int, float> {
    float priority(const int& k, const float& v1){
        return priority_map[k];
    }
};

struct KatzTermChecker : public TermChecker<int, float> {
    double last;

    KatzTermChecker(){
        curr = 0;
        last = -10000;
    }

    double partia_calculate(TermCheckIterator<int, float>* statetable){
        double partial_curr = 0;
        while(!statetable->done()){
            statetable->Next();
            partial_curr += statetable->value2();
        }
        return partial_curr;
    }

    bool terminate(vector<double> partials){
        curr = 0;
        vector<double>::iterator it;
        for(it=partials.begin(); it!=partials.end(); it++){
                double partial = *it;
                curr += partial;
        }

        if(curr-last < FLAGS_termcheck_threshold){
            return true;
        }else{
            last = curr;
            return false;
        }
    }
};

static int Katz3(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
	graph = CreateTable<int, float, float, vector<int> >(0, shards, FLAGS_portion, new Sharding::Mod,
		  new Accumulators<float>::Sum,
          new KatzScheduler,
          new KatzTermChecker);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("katz3_RunKernel1", "run", graph);
    for(int i=0; i<FLAGS_max_iterations; i++){
    	m.run_all("katz3_RunKernel2", "map", graph);
        m.run_all("katz3_RunKernel3", "map", graph);
    }
    m.run_all("katz3_RunKernel4", "run", graph);
  }
  return 0;
}
REGISTER_RUNNER(Katz3);

class katz3_RunKernel1 : public DSMKernel {
public:
      template <class TableA>
      void init_table(TableA* a){
          if(!graph->initialized()) graph->InitStateTable();
          graph->resize(FLAGS_num_nodes);

          string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
          ifstream inFile;
          inFile.open(patition_file.c_str());

          if (!inFile) {
            cerr << "Unable to open file" << patition_file;
            exit(1); // terminate with error
          }

            char line[1024000];
            while (inFile.getline(line, 1024000)) {
                string linestr(line);
                int pos = linestr.find("\t");
                int source = boost::lexical_cast<int>(linestr.substr(0, pos));
                string links = linestr.substr(pos+1);
                vector<int> linkvec = readUnWeightLinks(links);
                priority_map[source] = linkvec.size();

                if(source == FLAGS_katz_source){
                    graph->put(source, FLAGS_num_nodes/graph->num_shards(), -FLAGS_num_nodes/graph->num_shards(), linkvec);
                }else{
                    graph->put(source, 0, 0, linkvec);
                    graph->updateF1(source, 0);              //reset
                }
            }
      }

      void run() {
              VLOG(0) << "init table";
              init_table(graph);
      }
};

REGISTER_KERNEL(katz3_RunKernel1);
REGISTER_METHOD(katz3_RunKernel1, run);

class katz3_RunKernel2 : public DSMKernel {
public:
	  template <class K, class Value1, class Value2, class Value3>
	  void run_iter(const K& k, Value1 &v1, Value2 &v2, Value3 &v3) {
		  vector<int>::iterator it;

		  Value1* v1_copy = new Value1();
		  *v1_copy = v1;

		  //update & apply & send
		  graph->updateF1(k, 0);
		  graph->accumulateF2(k, *v1_copy);
		  for(it=v3.begin(); it!=v3.end(); it++){
			  int target = *it;
			  graph->accumulateF1(target, FLAGS_katz_beta * (*v1_copy));
		  }

          delete v1_copy;
	  }

	  template <class TableA>
	  void run_loop(TableA* a) {
		  typename TableA::Iterator *it = a->get_typed_iterator(current_shard(), false);

		  for (; !it->done(); it->Next()) {
			  run_iter(it->key(), it->value1(), it->value2(), it->value3());
		  }
		  delete it;
	  }

	  void map() {
		  VLOG(0) << "start map";
		  run_loop(graph);
	  }
};

REGISTER_KERNEL(katz3_RunKernel2);
REGISTER_METHOD(katz3_RunKernel2, map);

class katz3_RunKernel3 : public DSMKernel {
public:
	  template <class TableA>
	  void run_loop(TableA* a) {
		  int update = 0;
		  double totalF2 = 0;

		  const int num_shards = graph->num_shards();
                  for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
                      	totalF2 += graph->getF2(i);
			update++;
                  }

                iter++;
                cout << "iter " << iter << " elapsed " << iter_timer.elapsed() << " shard " << current_shard() <<
                                        " total F2 : " << totalF2 <<
                                        " update " << update << endl;
	  }

	  void map() {
		  VLOG(0) << "start map";
		  run_loop(graph);
	  }
};

REGISTER_KERNEL(katz3_RunKernel3);
REGISTER_METHOD(katz3_RunKernel3, map);

class katz3_RunKernel4 : public DSMKernel {
public:
	  template <class TableA>
	  void dump(TableA* a){
		  fstream File;

		  string file = StringPrintf("%s/part-%d", FLAGS_result_dir.c_str(), current_shard());
		  File.open(file.c_str(), ios::out);

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
	    	  File << i << "\t" << graph->getF1(i) << "|" << graph->getF2(i) << "\n";
	      };

	      File.close();
	  }

	  void run() {
          VLOG(0) << "dump table";
	      dump(graph);
	  }
};

REGISTER_KERNEL(katz3_RunKernel4);
REGISTER_METHOD(katz3_RunKernel4, run);

