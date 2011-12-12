#include "client/client.h"


using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int32(num_nodes);
DECLARE_double(portion);
DECLARE_double(termcheck_threshold);
DECLARE_int32(max_iterations);

static TypedGlobalTable<int, float, float, vector<int> > *pages;
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

struct PagerankTermChecker : public TermChecker<int, float> {

    PagerankTermChecker(){
        curr = 0;
    }
    
    double partia_calculate(TermCheckIterator<int, float>* statetable){
        float partial_curr = 0;
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
        
        if(curr > FLAGS_termcheck_threshold){
            return true;
        }else{
            return false;
        }
    }
};

static int Pagerank3(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
  pages = CreateTable<int, float, float, vector<int> >(0, shards, FLAGS_portion, new Sharding::Mod,
		  new Accumulators<float>::Sum, new Schedulers<int, float, float>::Direct, new PagerankTermChecker);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("pagerank3_RunKernel1", "run", pages);
    for(int i=0; i<FLAGS_max_iterations; i++){
        m.run_all("pagerank3_RunKernel2", "map", pages);
        m.run_all("pagerank3_RunKernel3", "map", pages);
    }
    m.run_all("pagerank3_RunKernel4", "run", pages);
  }
  return 0;
}
REGISTER_RUNNER(Pagerank3);

class pagerank3_RunKernel1 : public DSMKernel {
public:
      template <class TableA>
      void init_table(TableA* a){  
          if(!pages->initialized()) pages->InitStateTable();
          pages->resize(FLAGS_num_nodes);

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

                pages->put(source, 0.2, 0, linkvec);
            }
      }

      void run() {
              VLOG(0) << "init table";
              init_table(pages);
      }
};

REGISTER_KERNEL(pagerank3_RunKernel1);
REGISTER_METHOD(pagerank3_RunKernel1, run);

class pagerank3_RunKernel2 : public DSMKernel {
public:
	  template <class K, class Value1, class Value2, class Value3>
	  void run_iter(const K& k, Value1 &v1, Value2 &v2, Value3 &v3) {
		  int size = (int) v3.size();
		  vector<int>::iterator it;

		  Value1* v1_copy = new Value1();
		  *v1_copy = v1;

		  //update & apply & send
		  pages->updateF1(k, 0);
		  pages->accumulateF2(k, *v1_copy);
		  for(it=v3.begin(); it!=v3.end(); it++){
                      int target = *it;
                      pages->accumulateF1(target, *v1_copy * 0.8 / size);
		  }
                  
                  delete v1_copy;
	  }

	  template <class TableA>
	  void run_loop(TableA* a) {
                    typename TableA::Iterator *it = a->get_entirepass_iterator(current_shard());

                    for (; !it->done(); it->Next()) {
                      run_iter(it->key(), it->value1(), it->value2(), it->value3());
                    }
	  }

	  void map() {
              VLOG(0) << "start map";
	      run_loop(pages);
	  }
};

REGISTER_KERNEL(pagerank3_RunKernel2);
REGISTER_METHOD(pagerank3_RunKernel2, map);

class pagerank3_RunKernel3 : public DSMKernel {
public:
      template <class TableA>
      void run_loop(TableA* a) {
              int update = 0;
              double totalF2 = 0;
              const int num_shards = pages->num_shards();
              for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
                    totalF2 += pages->getF2(i);
                    update++;
              }

            iter++;
            cout << "iter " << iter << " elapsed " << iter_timer.elapsed() << " shard " << current_shard() <<
                                    " total F2 : " << totalF2 <<
                                    " update " << update << endl;
      }

      void map() {
              VLOG(0) << "start map";
              run_loop(pages);
      }
};

REGISTER_KERNEL(pagerank3_RunKernel3);
REGISTER_METHOD(pagerank3_RunKernel3, map);

class pagerank3_RunKernel4 : public DSMKernel {
public:
	  template <class TableA>
	  void dump(TableA* a){
		  fstream File;

		  string file = StringPrintf("%s/part-%d", FLAGS_result_dir.c_str(), current_shard());
		  File.open(file.c_str(), ios::out);

	      const int num_shards = pages->num_shards();
	      for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
	    	  File << i << "\t" << pages->getF1(i) << "|" << pages->getF2(i) << "\n";
	      };

	      File.close();
	  }

	  void run() {
              VLOG(0) << "dump table";
	      dump(pages);
	  }
};

REGISTER_KERNEL(pagerank3_RunKernel4);
REGISTER_METHOD(pagerank3_RunKernel4, run);

