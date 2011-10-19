#include "client/client.h"


using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int32(num_nodes);
DECLARE_double(portion);
DECLARE_double(termcheck_threshold);
DECLARE_int32(adsorption_starts);
DECLARE_double(adsorption_damping);


static TypedGlobalTable<int, float, float, vector<Link> > *graph;
static map<int, float> priority_map;

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

struct AdsorptionScheduler : public Scheduler<int, float> {
    float priority(const int& k, const float& v1){
        return v1*priority_map[k];
    }
};

struct AdsorptionTermChecker : public TermChecker<int, float> {
    double last;
    
    AdsorptionTermChecker(){
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

static int Adsorption2(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
  graph = CreateTable<int, float, float, vector<Link> >(0, shards, FLAGS_portion, new Sharding::Mod,
		  new Accumulators<float>::Sum, 
                  new AdsorptionScheduler, 
                  new AdsorptionTermChecker);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("adsorption2_RunKernel1", "run", graph);
    m.run_all("adsorption2_RunKernel2", "map", graph);
    m.run_all("adsorption2_RunKernel3", "run", graph);
  }
  return 0;
}
REGISTER_RUNNER(Adsorption2);

class adsorption2_RunKernel1 : public DSMKernel {
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
                vector<Link> linkvec = readWeightLinks(links);
                
                float total_weight = 0;
                vector<Link>::iterator it;
                for(it=linkvec.begin(); it!=linkvec.end(); it++){
                      Link target = *it;
                      total_weight += target.weight;
		}
                priority_map[source] = total_weight;

                if(source < FLAGS_adsorption_starts){
                    graph->put(source, 10, 0, linkvec);
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

REGISTER_KERNEL(adsorption2_RunKernel1);
REGISTER_METHOD(adsorption2_RunKernel1, run);

class adsorption2_RunKernel2 : public DSMKernel {
public:
	  template <class K, class Value1, class Value2, class Value3>
	  void run_iter(const K& k, Value1 &v1, Value2 &v2, Value3 &v3) {
		  vector<Link>::iterator it;

		  Value1* v1_copy = new Value1();
		  *v1_copy = v1;

		  //update & apply & send
		  graph->updateF1(k, 0);
		  graph->accumulateF2(k, *v1_copy);
		  for(it=v3.begin(); it!=v3.end(); it++){
                      Link target = *it;
                      graph->accumulateF1(target.end, *v1_copy * FLAGS_adsorption_damping * target.weight);
		  }
                  
                  delete v1_copy;
	  }

	  template <class TableA>
	  void run_loop(TableA* a) {
              while(true){
                typename TableA::Iterator *it = a->get_typed_iterator(current_shard(), false);
                if(it == NULL) break;

                for (; !it->done(); it->Next()) {
                    run_iter(it->key(), it->value1(), it->value2(), it->value3());
                }
                delete it;
              }
	  }

	  void map() {
              VLOG(0) << "start map";
	      run_loop(graph);
	  }
};

REGISTER_KERNEL(adsorption2_RunKernel2);
REGISTER_METHOD(adsorption2_RunKernel2, map);

class adsorption2_RunKernel3 : public DSMKernel {
public:
	  template <class TableA>
	  void dump(TableA* a){
		  double totalF1 = 0;
		  double totalF2 = 0;
		  fstream File;

		  string file = StringPrintf("%s/part-%d", FLAGS_result_dir.c_str(), current_shard());
		  File.open(file.c_str(), ios::out);

	      const int num_shards = graph->num_shards();
	      for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
	    	  totalF1 += graph->getF1(i);
	    	  totalF2 += graph->getF2(i);

	    	  File << i << "\t" << graph->getF1(i) << "|" << graph->getF2(i) << "\n";
	      };

	      File.close();

	      cout << "total F1 : " << totalF1 << endl;
	      cout << "total F2 : " << totalF2 << endl;
	  }

	  void run() {
              VLOG(0) << "dump table";
	      dump(graph);
	  }
};

REGISTER_KERNEL(adsorption2_RunKernel3);
REGISTER_METHOD(adsorption2_RunKernel3, run);

