#include "client/client.h"


using namespace dsm;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

struct PagerankIterateKernel : public IterateKernel<int, float, vector<int> > {
    float zero;


    PagerankIterateKernel() : zero(0){}

    void read_data(string& line, int& k, vector<int>& data){
        string linestr(line);
        int pos = linestr.find("\t");
        if(pos == -1) return;
        
        int source = boost::lexical_cast<int>(linestr.substr(0, pos));

        vector<int> linkvec;
        string links = linestr.substr(pos+1);
        //cout<<"links:"<<links<<endl;
        int spacepos = 0;
        while((spacepos = links.find_first_of(" ")) != links.npos){
            int to;
            if(spacepos > 0){
                to = boost::lexical_cast<int>(links.substr(0, spacepos));
                //cout<<"to:"<<to<<endl;
            }
            links = links.substr(spacepos+1);
            linkvec.push_back(to);
        }

        k = source;
        data = linkvec;
    }

    void init_c(const int& k, float& delta,vector<int>& data){
        delta = 0.2;
    }

    void init_v(const int& k,float& v,vector<int>& data){
        v=0;  
    }
    
    void accumulate(float& a, const float& b){
            a = a + b;
    }

    void priority(float& pri, const float& value, const float& delta){
            pri = delta;
    }

    void g_func(const int& k,const float& delta, const float&value, const vector<int>& data, vector<pair<int, float> >* output){
            int size = (int) data.size();
            float outv = delta * 0.8 / size;
            
            //cout << "size " << size << endl;
            for(vector<int>::const_iterator it=data.begin(); it!=data.end(); it++){
                    int target = *it;
                    output->push_back(make_pair(target, outv));
            }
    }

    const float& default_v() const {
            return zero;
    }
};


static int Pagerank(ConfigData& conf) {
    MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel<int, float, vector<int> >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharding::Mod,
                                        new PagerankIterateKernel,
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
