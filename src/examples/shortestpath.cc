#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DEFINE_int64(shortestpath_source, 0, "");

struct Link{
	Link(int inend, float inweight) :
			end(inend), weight(inweight){
	}
	int end;
	float weight;
};
bool operator==(const Link& lth, const Link& rth){
	return lth.end == rth.end;
}
bool operator==(const Link& lth, const int rth){
	return lth.end == rth;
}

struct ShortestpathIterateKernel: public IterateKernel<int, float, vector<Link> > {
	float imax;

	ShortestpathIterateKernel(){
		imax = std::numeric_limits<float>::max();
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
	}
	void read_init(std::string& line, int& k, float& delta, float& value){
		// format: "<key>\t<value>:<delta>"
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

	vector<int> get_keys(const vector<Link>& data){
		vector<int> res;
		res.reserve(data.size());
		for(const Link& l : data){
			res.push_back(l.end);
		}
		return res;
	}

	void init_c(const int& k, float& delta, vector<Link>&){
		if(k == FLAGS_shortestpath_source){
			delta = 0;
		}else{
			delta = imax;
		}
	}

	void init_v(const int& k, float& v, vector<Link>&){
		v = default_v();
	}

	void accumulate(float& a, const float& b){
		a = std::min(a, b);
	}
	bool better(const float& a, const float& b){
		return a < b;
	}
	bool is_minmax_accumulate() const{
		return true;
	}

	void priority(float& pri, const float& value, const float& delta, const vector<Link>& data){
		pri = value - std::min(value, delta);
	}

	void g_func(const int& k, const float& delta, const float& value, const vector<Link>& data,
			vector<pair<int, float> >* output){
		for(vector<Link>::const_iterator it = data.begin(); it != data.end(); it++){
			if(it->end == FLAGS_shortestpath_source){	// to avoid positive loop
				continue;
				//output->push_back(make_pair(it->end, default_v()));
			}
			float outv = delta + it->weight;
			output->push_back(make_pair(it->end, outv));
		}
	}

	const float& default_v() const{
		return imax;
	}
};

static int Shortestpath(ConfigData& conf){
	Sharders::Mod vS;
	ShortestpathIterateKernel vSIK;
	TermCheckers<int, float>::Diff vTC;
	MaiterKernel<int, float, vector<Link> >* kernel = new MaiterKernel<int, float, vector<Link> >(
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

REGISTER_RUNNER(Shortestpath);

