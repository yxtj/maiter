#include "client/client.h"

using namespace dsm;
using namespace std;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

DECLARE_bool(priority_diff);
DECLARE_double(weight_alpha);
DECLARE_bool(priority_degree);

struct PagerankIterateKernel: public IterateKernel<int, float, vector<int> > {
	const float zero=0.0f;

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
	void read_init(std::string& line, int& k, float& delta, float& value){
		// format: "<key>\t<delta>:<value>"
		size_t p=line.find('\t');
		k = stoi(line.substr(0, p));
		++p;
		size_t p2=line.find(':', p);
		delta = stof(line.substr(p, p2-p));
		value = stof(line.substr(p2+1));
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
	vector<int> get_keys(const vector<Link>& data){
		vector<int> res;
		res.reserve(data.size());
		for(const Link& l : data){
			res.push_back(l.end);
		}
		return res;
	}

	void init_c(const int& k, float& delta, vector<int>& data){
		delta = 0.2;
	}
	void init_v(const int& k, float& v, vector<int>& data){
		v = default_v();
	}

	void accumulate(float& a, const float& b){
		a = a + b;
	}
	bool better(const float& a, const float& b){
		return true;
	}

	void priority(float& pri, const float& value, const float& delta, const vector<int>& data){
		// delta is d_v, value is v_i
		if(FLAGS_priority_diff){
			pri = delta;
		}else{
			pri = delta < 0 ? value : FLAGS_weight_alpha * value;
		}
		if(FLAGS_priority_degree)
			pri *= data.size();
	}
	float g_func(const int& k, const float& delta, const float&value, const vector<int>& data, const int& dst){
		int size = (int)data.size();
		return delta * 0.8 / size;
	}
	void g_func(const int& k, const float& delta, const float&value, const vector<int>& data,
			vector<pair<int, float> >* output){
		int size = (int)data.size();
		float outv = delta * 0.8 / size;

		//cout << "size " << size << endl;
		for(vector<int>::const_iterator it = data.begin(); it != data.end(); it++){
			output->push_back(make_pair(*it, outv));
		}
	}

	const float& default_v() const{
		return zero;
	}
};

static int Pagerank(ConfigData& conf){
	Sharders::Mod vS;
	PagerankIterateKernel vRIK;
	TermCheckers<int, float>::Diff vTC;
	MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel<int, float, vector<int> >(
			conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
			&vS, &vRIK, &vTC);

	kernel->registerMaiter();

	if(!StartWorker(conf)){
		Master m(conf);
		m.run_maiter(kernel);
	}

	delete kernel;
	return 0;
}

REGISTER_RUNNER(Pagerank);
