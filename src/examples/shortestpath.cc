#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DECLARE_int64(graph_source);

DECLARE_bool(priority_diff);
DECLARE_double(weight_alpha);
DECLARE_bool(priority_degree);

struct ShortestPathIterateKernel: public IterateKernel<int, float, vector<Link> > {
	float imax;

	ShortestPathIterateKernel(){
		imax = std::numeric_limits<float>::infinity();
	}

	void read_data(string& line, int& k, vector<Link>& data){
		//line: "k\tai,aw bi,bw ci,cw "
		size_t pos = line.find('\t');

		k = stoi(line.substr(0, pos));
		++pos;

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
		// special process for the source node: add a self-loop with 0 weight,
		// to make sure that the delta for the source node can always be zero.
		if(k==FLAGS_graph_source){
			auto it=find_if(data.begin(), data.end(), [&](const Link& p){
				return p.end==FLAGS_graph_source;
			});
			if(it==data.end())
				data.push_back(Link(FLAGS_graph_source, 0));
			else
				it->weight=0;
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
		if(k == FLAGS_graph_source){
			delta = 0;
		}else{
			delta = imax;
		}
	}

	void init_v(const int& k, float& value, vector<Link>&){
		value = imax;
	}

	void accumulate(float& a, const float& b){
		a = std::min(a, b);
	}
	bool better(const float& a, const float& b){
		return a < b;
	}
	bool is_selective() const{
		return true;
	}

	void priority(float& pri, const float& value, const float& delta, const vector<Link>& data){
		// delta is u_i, value is v_i
		if(FLAGS_priority_diff){
			pri = delta - value;
		}else{
			// TODO: because better is <, pri should be its inverse (-pri)
			pri = better(delta, value) ? delta : FLAGS_weight_alpha * delta;
		}
		if(FLAGS_priority_degree)
			pri *= data.size();

	}

	float g_func(const int& k, const float& delta, const float& value, const vector<Link>& data, const int& dst){
		auto it=find_if(data.begin(), data.end(), [&](const Link& l){
			return l.end == dst;
		});
		return delta+it->weight;
		//return d.end==FLAGS_graph_source ? imax : delta+it->weight;
	}

	void g_func(const int& k, const float& delta, const float& value, const vector<Link>& data,
			vector<pair<int, float> >* output){
		for(vector<Link>::const_iterator it = data.begin(); it != data.end(); it++){
			output->push_back(make_pair(it->end, delta + it->weight));
			/*
			if(it->end == FLAGS_graph_source){	// to avoid positive loop
				output->push_back(make_pair(it->end, imax));
			}else{
				float outv = delta + it->weight;
				output->push_back(make_pair(it->end, outv));
			} */
		}
	}

	const float& default_v() const{
		return imax;
	}
};

static int ShortestPath(ConfigData& conf){
	Sharders::Mod vS;
	ShortestPathIterateKernel vSIK;
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

REGISTER_RUNNER(ShortestPath);

