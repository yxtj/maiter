#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

struct PagerankIterateKernel : public IterateKernel<int, int, vector<int> > {
    int zero;


    PagerankIterateKernel() : zero(0){}

    void read_data(string& line, int& k, vector<int>& data){
		//line: "k\ta b c "
		size_t pos = line.find('\t');

		k = stoi(line.substr(0, pos));
		++pos;

		data.clear();
		size_t spacepos;
		while((spacepos = line.find(' ',pos)) != line.npos){
			int to = stoi(line.substr(pos, spacepos-pos));
			data.push_back(to);
			pos=spacepos+1;
		}

	}

    void init_c(const int& k, int& delta, vector<int>& data){
            int  init_delta = k;
            delta = init_delta;
    }

    void init_v(const int& k,int& v,vector<int>& data){
            v=0;
    }
    void accumulate(int& a, const int& b){
            a=std::max(a,b);
    }

    void priority(int& pri, const int& value, const int& delta){
            pri = value-std::max(value,delta);
    }

    void g_func(const int& k,const int& delta, const int& value, const vector<int>& data, vector<pair<int, int> >* output){
            int outv = value;
            
            //cout << "size " << size << endl;
            for(vector<int>::const_iterator it=data.begin(); it!=data.end(); it++){
                    int target = *it;
                    output->push_back(make_pair(target, outv));
            }
    }

    const int& default_v() const {
        return zero;
    }
};


static int Pagerank(ConfigData& conf) {
    MaiterKernel<int, int, vector<int> >* kernel = new MaiterKernel<int, int, vector<int> >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharders::Mod,
                                        new PagerankIterateKernel,
                                        new TermCheckers<int, int>::Diff);
    
    
    kernel->registerMaiter();

    if (!StartWorker(conf)) {
        Master m(conf);
        m.run_maiter(kernel);
    }
    
    delete kernel;
    return 0;
}

REGISTER_RUNNER(Pagerank);

