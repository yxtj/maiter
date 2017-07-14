#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DEFINE_int32(adsorption_starts, 100, "");
DEFINE_double(adsorption_damping, 0.1, "");

struct Link{
	Link(int inend, float inweight) : end(inend), weight(inweight){}
	int end;
	float weight;
};

struct AdsorptionIterateKernel: public IterateKernel<int, float, vector<Link> > {

	float zero;

	AdsorptionIterateKernel() :
			zero(0){
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
			Link to(stoi(line.substr(pos, cut - pos)),
					stof(line.substr(cut + 1, spacepos - cut - 1)));
			data.push_back(to);
			pos = spacepos + 1;
		}

	}

	void init_c(const int& k, float& delta, vector<Link>& ){
		if(k < FLAGS_adsorption_starts){
			delta = 10;
		}else{
			delta = 0;
		}
	}

	void init_v(const int& k, float& v, vector<Link>& ){
		v=default_v();
	}

	void accumulate(float& a, const float& b){
		a = a + b;
	}

	void priority(float& pri, const float& value, const float& delta){
		pri = delta;
	}

	void g_func(const int& k, const float& delta, const float& value, const vector<Link>& data, vector<pair<int, float> >* output){
		for(vector<Link>::const_iterator it = data.begin(); it != data.end(); it++){
			Link target = *it;
			float outv = delta * FLAGS_adsorption_damping * target.weight;
			output->push_back(make_pair(target.end, outv));
		}
	}

	const float& default_v() const{
		return zero;
	}
};

static int Adsorption(ConfigData& conf){
	MaiterKernel<int, float, vector<Link> >* kernel = new MaiterKernel<int, float, vector<Link> >(
			conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
			new Sharders::Mod,
			new AdsorptionIterateKernel,
			new TermCheckers<int, float>::Diff);

	kernel->registerMaiter();

	if(!StartWorker(conf)){
		Master m(conf);
		m.run_maiter(kernel);
	}

	delete kernel;
	return 0;
}

REGISTER_RUNNER(Adsorption);

