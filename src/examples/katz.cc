#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);
DEFINE_int64(katz_source, 0, "");
DEFINE_double(katz_beta, 0.1, "");

struct KatzIterateKernel: public IterateKernel<int, float, vector<int> > {

	float zero;

	KatzIterateKernel() :
			zero(0){
	}

	void read_data(string& line, int& k, vector<int>& data){
		//line: "k\ta b c "
		size_t pos = line.find('\t');

		k = stoi(line.substr(0, pos));
		++pos;

		data.clear();
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			int to = stoi(line.substr(pos, spacepos - pos));
			data.push_back(to);
			pos = spacepos + 1;
		}

	}

	void init_c(const int& k, float& delta, vector<int>&){
		if(k == FLAGS_katz_source){
			delta = 1000000;
		}else{
			delta = 0;
		}
	}

	void init_v(const int& k, float& v, vector<int>&){
		v = default_v();
	}

	void accumulate(float& a, const float& b){
		a = a + b;
	}

	void priority(float& pri, const float& value, const float& delta){
		pri = delta;
	}

	void g_func(const int& k, const float& delta, const float& value, const vector<int>& data,
			vector<pair<int, float> >* output){
		float outv = FLAGS_katz_beta * delta;
		for(vector<int>::const_iterator it = data.begin(); it != data.end(); it++){
			int target = *it;
			output->push_back(make_pair(target, outv));
		}
	}

	const float& default_v() const{
		return zero;
	}
};

static int Katz(ConfigData& conf){
	MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel<int, float, vector<int> >(
			conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
			new Sharders::Mod,
			new KatzIterateKernel,
			new TermCheckers<int, float>::Diff);

	kernel->registerMaiter();

	if(!StartWorker(conf)){
		Master m(conf);
		m.run_maiter(kernel);
	}

	delete kernel;
	return 0;
}

REGISTER_RUNNER(Katz);

