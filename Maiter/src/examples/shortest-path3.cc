#include "client/client.h"
#include <limits>

using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int32(num_nodes);
DECLARE_double(portion);
DECLARE_int32(shortestpath_source);
DECLARE_int32(max_iterations);


static TypedGlobalTable<int, float, float, vector<Link> > *graph;
static int iter = 0;
static Timer iter_timer;

static vector<Link> readWeightLinks(string links){
    vector<Link> linkvec;
    int spacepos = 0;
    while((spacepos = links.find_first_of(" ")) != links.npos){
        Link to(0, 0);
        if(spacepos > 0){
            string link = links.substr(0, spacepos);
            int cut = links.find_first_of(",");
            to.end = boost::lexical_cast<int>(link.substr(0, cut));
            to.weight = boost::lexical_cast<float>(link.substr(cut+1));
        }
        links = links.substr(spacepos+1);
        linkvec.push_back(to);
    }
    
    return linkvec;
}

struct ShortestPathTermChecker : public TermChecker<int, float> {
    double last;
    
    ShortestPathTermChecker(){
        last = -1;
        curr = 0;
    }
    
    double partia_calculate(TermCheckIterator<int, float>* statetable){
        double partial_curr = 0;
        float imax = std::numeric_limits<float>::max();
        while(!statetable->done()){
            statetable->Next();
            if(statetable->value2() != imax){
                partial_curr += static_cast<double>(statetable->value2());
            }
        }
        return partial_curr;
    }
    
    bool terminate(vector<double> partials){
        curr = 0;
        vector<double>::iterator it;
        for(it=partials.begin(); it!=partials.end(); it++){
                curr += *it;
        }
        
        if(abs(curr - last) < 0.0001){
            return true;
        }else{
            last = curr;
            return false;
        }
    }
};

static int Shortest_Path3(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
  graph = CreateTable<int, float, float, vector<Link> >(0, shards, FLAGS_portion, new Sharding::Mod,
		  new Accumulators<float>::Min, new Schedulers<int, float>::Opposite, new ShortestPathTermChecker);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("shortest_path3_RunKernel1", "run", graph);
    for(int i=0; i<FLAGS_max_iterations; i++){
        m.run_all("shortest_path3_RunKernel2", "map", graph);
        m.run_all("shortest_path3_RunKernel3", "map", graph);
    }
    m.run_all("shortest_path3_RunKernel4", "run", graph);
  }
  return 0;
}
REGISTER_RUNNER(Shortest_Path3);

class shortest_path3_RunKernel1 : public DSMKernel {
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

          float imax = std::numeric_limits<float>::max();
            char line[1024000];
            while (inFile.getline(line, 1024000)) {
                string linestr(line);
                int pos = linestr.find("\t");
                int source = boost::lexical_cast<int>(linestr.substr(0, pos));
                string links = linestr.substr(pos+1);
                vector<Link> linkvec = readWeightLinks(links);

                if(source == FLAGS_shortestpath_source){
                    graph->put(source, 0, imax, linkvec);
                }else{
                    graph->put(source, imax, imax, linkvec);
                    graph->updateF1(source, imax);              //reset
                }
            }
      }

      void run() {
              VLOG(0) << "init table";
              init_table(graph);
      }
};

REGISTER_KERNEL(shortest_path3_RunKernel1);
REGISTER_METHOD(shortest_path3_RunKernel1, run);

class shortest_path3_RunKernel2 : public DSMKernel {
public:
      template <class K, class Value1, class Value2, class Value3>
      void run_iter(const K& k, Value1 &v1, Value2 &v2, Value3 &v3) {
          float imax = std::numeric_limits<float>::max();
          //cout << "input tuples " << k << "\t" << v1 << "\t" << v2 << endl;
          if(v1 >= v2){
              graph->updateF1(k, imax);
              return;               //judge if the update is eligible
          }
          
          vector<Link>::iterator it;

          Value1* v1_copy = new Value1();
          *v1_copy = v1;

          //update & apply & send
          graph->updateF1(k, imax);
          graph->accumulateF2(k, *v1_copy);
          //cout << "accumulate on F2 " << k << " with " << *v1_copy << endl;
          for(it=v3.begin(); it!=v3.end(); it++){
              Link target = *it;
              graph->accumulateF1(target.end, *v1_copy + target.weight);
              //cout << "accumulate on " << target.end << " with " << *v1_copy + target.weight << endl;
          }

          delete v1_copy;
      }

      template <class TableA>
      void run_loop(TableA* a) {
            typename TableA::Iterator *it = a->get_entirepass_iterator(current_shard());
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

REGISTER_KERNEL(shortest_path3_RunKernel2);
REGISTER_METHOD(shortest_path3_RunKernel2, map);

class shortest_path3_RunKernel3 : public DSMKernel {
public:
	  template <class TableA>
	  void run_loop(TableA* a) {
		  int update = 0;
		  double totalF2 = 0;
                  float imax = std::numeric_limits<float>::max();

		  const int num_shards = graph->num_shards();
                  for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
                      if(graph->getF2(i) != imax) totalF2 += graph->getF2(i);
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

REGISTER_KERNEL(shortest_path3_RunKernel3);
REGISTER_METHOD(shortest_path3_RunKernel3, map);

class shortest_path3_RunKernel4 : public DSMKernel {
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

REGISTER_KERNEL(shortest_path3_RunKernel4);
REGISTER_METHOD(shortest_path3_RunKernel4, run);
