#include "client/client.h"
#include "maiterkernel.h"


using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DECLARE_double(termcheck_threshold);

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

struct PagerankInitializer : public Initializer<int, float, vector<int> > {
    void initTable(int shard_id, TypedGlobalTable<int, float, float, vector<int> >* table){
        if(!table->initialized()) table->InitStateTable();
            table->resize(FLAGS_num_nodes);

            string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), shard_id);
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

            table->put(source, 0.2, 0, linkvec);
        }
    }
};

struct PagerankSender : public Sender<int, float, vector<int> > {
    void send(const float& delta, const vector<int>& data, MaiterKernel<int, float, vector<int> >* output){
        int size = (int) data.size();
        vector<int>::iterator it;

        float outv = delta * 0.8 / size;
        for(it=data.begin(); it!=data.end(); it++){
            int target = *it;
            output->sendto(target, outv);
        }
    }

    float reset(const int& k, const float& delta){
        return 0;
    }
};

struct PagerankTermChecker : public TermChecker<int, float> {
    static double curr = 0;

    double set_curr(){
        return curr;
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

static int Pagerank(ConfigData& conf) {
    MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel(conf, FLAGS_portion, FLAGS_result_dir,
                                        new Sharding::Mod,
                                        new PagerankInitializer,
                                        new Accumulators<float>::Sum,
                                        new PagerankSender,
                                        new PagerankTermChecker);

    kernel->conf = conf;
    kernel->schedule_portion = FLAGS_portion;
    kernel->output = FLAGS_result_dir;
    kernel->sharder = new Sharding::Mod;
    kernel->initializer = new PagerankInitializer;
    kernel->accum = new Accumulators<float>::Sum;
    kernel->sender = new PagerankSender;
    kernel->termchecker = new PagerankTermChecker;
    
    kernel->run();

    return 0;
}

REGISTER_RUNNER(Pagerank);
