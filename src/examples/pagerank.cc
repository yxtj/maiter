#include "client/client.h"

using namespace dsm;
using namespace std;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

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

	void init_c(const int& k, float& delta, vector<int>& data){
		delta = 0.2;
	}

	void init_v(const int& k, float& v, vector<int>& data){
		v = default_v();
	}

	void accumulate(float& a, const float& b){
		a = a + b;
	}

	void priority(float& pri, const float& value, const float& delta){
		pri = delta;
	}

	void g_func(const int& k, const float& delta, const float&value, const vector<int>& data,
			vector<pair<int, float> >* output){
		int size = (int)data.size();
		float outv = delta * 0.8 / size;

		//cout << "size " << size << endl;
		for(vector<int>::const_iterator it = data.begin(); it != data.end(); it++){
			int target = *it;
			output->push_back(make_pair(target, outv));
		}
	}

	const float& default_v() const{
		return zero;
	}
};

int Pagerank(ConfigData& conf){
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

//REGISTER_RUNNER(Pagerank);
