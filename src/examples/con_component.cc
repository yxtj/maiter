#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DECLARE_double(weight_alpha);
DECLARE_bool(priority_degree);

struct ConCompIterateKernel : public IterateKernel<int, int, vector<int> > {
    const int zero;

    ConCompIterateKernel() : zero(0){}

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
		// add a self loop edge to maintain correctness, especially running with an evolving graph
		auto it=find(data.begin(), data.end(), k);
		if(it==data.end())
			data.push_back(k);
	}
	void read_init(std::string& line, int& k, int& delta, int& value){
		// format: "<key>\t<delta>:<value>"
		size_t p=line.find('\t');
		k = stoi(line.substr(0, p));
		++p;
		size_t p2=line.find(':', p);
		delta = stoi(line.substr(p, p2-p));
		value = stoi(line.substr(p2+1));
	}

	void read_change(std::string& line, int& k, ChangeEdgeType& type, vector<int>& change){
		// line: "<type>\t<src>,<dst>"
		// <type> is one of A, R, I, D
		switch(line[0]){
			case 'A': type=ChangeEdgeType::ADD;	break;
			case 'R': type=ChangeEdgeType::REMOVE;	break;
			case 'I': type=ChangeEdgeType::INCREASE;	break;
			case 'D': type=ChangeEdgeType::DECREASE;	break;
			default: LOG(FATAL)<<"Cannot parse change line: "<<line;
		}
		size_t p1=line.find(',', 2);
		k=stoi(line.substr(2,p1-2));
		//size_t p2=line.find(',', p1+1);
		//int dst=stoi(line.substr(p1+1, p2-p1-1));
		int dst=stoi(line.substr(p1+1));
		//float weight=stof(line.substr(p2+1));
		change.clear();
		//change.emplace_back(dst, weight);
		change.push_back(dst);
	}

	int get_key(const int& d){
		return d;
	}
	std::vector<int> get_keys(const vector<int>& d){
		return d;
	}

    void init_c(const int& k, int& delta, vector<int>& data){
		delta = k;
    }
    void init_v(const int& k,int& value,vector<int>& data){
		value = -1;
    }

    void accumulate(int& a, const int& b){
		a=std::max(a,b);
    }
	bool better(const int& a, const int& b){
		return a > b;
    }
	bool is_selective() const{
		return true;
	}

    void priority(int& pri, const int& value, const int& delta, const vector<int>& data){
		//pri = value-std::max(value,delta);
		int dif = (value - delta) * (FLAGS_priority_degree? data.size() : 1);
		if(dif<=0)	// good news
			pri = -dif;
		else
			pri = static_cast<int>(FLAGS_weight_alpha * dif);
    }

    int g_func(const int& k,const int& delta, const int& value, const int& d){
    	return delta;
    }
    void g_func(const int& k,const int& delta, const int& value, const vector<int>& data,
    		vector<pair<int, int> >* output)
    {
		for(vector<int>::const_iterator it=data.begin(); it!=data.end(); ++it){
			output->push_back(make_pair(*it, delta));
		}
    }

    const int& default_v() const {
        return zero;
    }
};


static int ConComp(ConfigData& conf) {
    MaiterKernel<int, int, vector<int> >* kernel = new MaiterKernel<int, int, vector<int> >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharders::Mod,
                                        new ConCompIterateKernel,
                                        new TermCheckers<int, int>::Diff);
    
    
    kernel->registerMaiter();

    if (!StartWorker(conf)) {
        Master m(conf);
        m.run_maiter(kernel);
    }
    
    delete kernel;
    return 0;
}

REGISTER_RUNNER(ConComp);

