#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

DECLARE_bool(priority_diff);
DECLARE_double(weight_alpha);
DECLARE_bool(priority_degree);

struct MarkovChainIterateKernel: public IterateKernel<int, float, vector<Link> > {
	const float zero;
    // static constexpr float Aii = 1.0f;
    // static constexpr float bi = 0.2f;

	MarkovChainIterateKernel(): zero(0.0f)
    {}

	void read_data(string& line, int& k, vector<Link>& data){
		//line: "k\tai,aw bi,bw ci,cw "
		size_t pos = line.find('\t');

		k = stoi(line.substr(0, pos));
		++pos;
		float sum = 0.0;
		data.clear();
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			size_t cut = line.find(',', pos + 1);
			int node=stoi(line.substr(pos, cut - pos));
			float weight=stof(line.substr(cut + 1, spacepos - cut - 1));
			Link to(node, weight);
			data.push_back(to);
			pos = spacepos + 1;
		}
		// normalize the distribution
		for(auto& l : data)
			l.weight /= sum;
	}
	void read_init(std::string& line, int& k, float& delta, float& value){
		// format: "<key>\t<delta>:<value>"
		size_t p=line.find('\t');
		k = stoi(line.substr(0, p));
		++p;
		size_t p2=line.find(':', p);
		delta = stof(line.substr(p, p2-p));
		value = stof(line.substr(p2+1));
	}

	void read_change(std::string& line, int& k, ChangeEdgeType& type, vector<Link>& change){
		// line: "<type>\t<src>,<dst>,<weight>"
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
		size_t p2=line.find(',', p1+1);
		int dst=stoi(line.substr(p1+1, p2-p1-1));
		float weight=stof(line.substr(p2+1));
		change.clear();
		change.emplace_back(dst, weight);
	}

	virtual int get_key(const Link& d){
		return d.end;
	}
	virtual vector<int> get_keys(const vector<Link>& data){
		vector<int> res;
		res.reserve(data.size());
		for(const Link& l : data){
			res.push_back(l.end);
		}
		return res;
	}

	void init_c(const int& k, float& delta, vector<Link>&){
        delta = 1; // initial probability (scale N times)
	}

	void init_v(const int& k, float& value, vector<Link>&){
		value = zero;
	}

	bool better(const float& a, const float& b){
		return true;
	}

	void accumulate(float& a, const float& b){
		a = a + b;
	}

	void priority(float& pri, const float& value, const float& delta, const vector<Link>& data){
		// delta is d_v, value is v_i
		if(FLAGS_priority_diff){
			pri = delta;
		}else{
			pri = delta < 0 ? value : FLAGS_weight_alpha * value;
		}
		if(FLAGS_priority_degree)
			pri *= data.size();
	}

	float g_func(const int& k, const float& delta, const float& value, const vector<Link>& data, const int& dst){
		auto it=find_if(data.begin(), data.end(), [&](const Link& l){
			return l.end == dst;
		});
		return it->weight * delta;
	}

	void g_func(const int& k, const float& delta, const float& value, const vector<Link>& data,
			vector<pair<int, float> >* output){
		for(vector<Link>::const_iterator it = data.begin(); it != data.end(); it++){
			output->push_back(make_pair(it->end, it->weight * delta));
		}
	}

	const float& default_v() const{
		return zero;
	}
};

static int MarkovChain(ConfigData& conf){
	Sharders::Mod vS;
	MarkovChainIterateKernel vSIK;
	TermCheckers<int, float>::Diff vTC;
	MaiterKernel<int, float, vector<Link> >* kernel =
		new MaiterKernel<int, float, vector<Link> >(
			conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
			&vS, &vSIK, &vTC);

	kernel->registerMaiter();

	if(!StartWorker(conf)){
		Master m(conf);
		m.run_maiter(kernel);
	}

	delete kernel;
	return 0;
}

REGISTER_RUNNER(MarkovChain);
