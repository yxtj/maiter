#include "client/client.h"


using namespace dsm;

DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

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
    void initTable(TypedGlobalTable<int, float, float, vector<int> >* table, int shard_id){
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
    float zero;
    
    PagerankSender() : zero(0){}
    
    void send(const float& delta, const vector<int>& data, vector<pair<int, float> >* output){
        int size = (int) data.size();
        float outv = delta * 0.8 / size;
        for(vector<int>::const_iterator it=data.begin(); it!=data.end(); it++){
            int target = *it;
            output->push_back(make_pair(target, outv));
        }
    }

    const float& reset() const {
        return zero;
    }
};


static int Pagerank(ConfigData& conf) {
    MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel<int, float, vector<int> >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharding::Mod,
                                        new PagerankInitializer,
                                        new Accumulators<float>::Sum,
                                        new PagerankSender,
                                        new TermCheckers<int, float>::Diff);
    
    
    kernel->registerMaiter();

    if (!StartWorker(conf)) {
        Master m(conf);
        m.run_maiter(kernel);
    }
    
    delete kernel;
    return 0;
}

REGISTER_RUNNER(Pagerank);
