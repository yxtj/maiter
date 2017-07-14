#include "client/client.h"
#include <string>

using namespace dsm;
using namespace std;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

struct Simrankiterate: public IterateKernel<string, double, vector<vector<int> > > {
	double zero;
	int count;
	Simrankiterate() :
			zero(0.0), count(0){
	}

	vector<int> read_link(string& line, size_t start_p, size_t end_p){
		vector<int> res;
		size_t spacepos;
		//cout<<"link_a:"<<links_a<<endl;
		while((spacepos = line.find(' ', start_p)) != line.npos && spacepos < end_p){
			int to = stoi(line.substr(start_p, spacepos - start_p));
			//cout<<"to:"<<to<<endl;
			res.push_back(to);
			start_p = spacepos + 1;
		}
		return res;
	}

	void read_data(string& line, string& k, vector<vector<int> >& data){
		//line: "k\tid \tlink_a\tlink_b"
		//link_a/b: "a b c "
		size_t pos = line.find('\t');
		k = line.substr(0, pos);

		size_t pos1 = line.find(' ', pos + 1);
		vector<int> tmp;
		tmp.push_back(stoi(line.substr(pos + 1, pos1)));
		data.push_back(tmp);

		pos = pos1 + 1;
		pos1 = line.find('\t', pos);
		data.push_back(read_link(line, pos, pos1));
		data.push_back(read_link(line, pos1 + 1, line.size()));
	}

	void read_data2(string& line, string& k, vector<vector<int> >& data){
		//line: "k\ta \tlink_a\tlink_b"
		//link_a/b: "a b c "
		string linestr(line);
		int pos = linestr.find('\t');
		if(pos == -1) return;
		k = linestr.substr(0, pos);

		vector<vector<int> > linkvec;
		string remain = linestr.substr(pos + 1);
		int pos1 = remain.find(" ");
		string I_ab = remain.substr(0, pos1);
		int i_ab = stoi(I_ab);
		vector<int> tmp;
		tmp.push_back(i_ab);
		linkvec.push_back(tmp);
		//cout<<linkvec[0][0]<<endl;
		pos = pos1;
		remain = remain.substr(pos + 1);
		//cout<<remain<<endl;
		pos1 = remain.find("\t");
		string links_a = remain.substr(0, pos1);
		string links_b = remain.substr(pos1 + 1);
		//cout<<links_a<<endl;
		//cout<<links_b<<endl;
		if(links_a.empty() || links_b.empty()){
			//cout<<"全空"<<endl;
			data = linkvec;
			return;
		}
		int spacepos = 0;
		vector<int> tmp_a;
		//cout<<"link_a:"<<links_a<<endl;
		while((spacepos = links_a.find_first_of(" ")) != links_a.npos){
			int to;
			if(spacepos > 0){
				to = stoi(links_a.substr(0, spacepos));
				//cout<<"to:"<<to<<endl;
			}
			links_a = links_a.substr(spacepos + 1);
			tmp_a.push_back(to);
		}
		linkvec.push_back(tmp_a);
		spacepos = 0;
		vector<int> tmp_b;
		while((spacepos = links_b.find_first_of(" ")) != links_b.npos){
			int to;
			if(spacepos > 0){
				to = stoi(links_b.substr(0, spacepos));
				// cout<<"to:"<<to<<endl;
			}
			links_b = links_b.substr(spacepos + 1);
			tmp_b.push_back(to);
		}
		linkvec.push_back(tmp_b);
		data = linkvec;
		//cout<<"read_data"<<endl;
	}

	void init_c(const string& key, double& delta, vector<vector<int> >& data){
		// cout<<"int_c1"<<endl;
		int pos = key.find("_");
		string key_a = key.substr(0, pos);
		string key_b = key.substr(pos + 1);
		if(key_a == key_b){
			if(data[0][0] == 0){
				delta = 1;
			}else{
				delta = data[0][0] / 0.8;
			}
		}else{
			delta = 0;
		}
		// cout<<"int_c"<<endl;

	}
	void init_v(const string& k, double& v, vector<vector<int> >& ){
		v = default_v();
	}
	void process_delta_v(const string& k, double& delta, double& value, vector<vector<int> >& data){
		if(count == 0) return;
		if(delta == 0) return;
		int I_ab = data[0][0];
		if(I_ab == 0) return;
		//value=value-delta;
		//cout<<"data[0]:"<<data[0]<<endl;
		//cout<<"delta_v:"<<delta<<endl;
		//value=(0.8*delta)/I_ab;
		//cout<<"delta:"<<delta<<endl;
		delta = (delta * 0.8) / I_ab;
		// cout<<"process_delta_V"<<endl;

	}

	void accumulate(double& a, const double& b){
		a = a + b;
	}

	void priority(double& pri, const double& value, const double& delta){
		pri = delta;
	}

	void g_func(const string& k, const double& delta, const double&value,
			const vector<vector<int> >& data, vector<pair<string, double> >* output){
		if(data.size() < 3){
			return;
		}
		string key = k;
		count++;
		//cout<<"v:"<<value<<endl;
		if(delta <= 0.0001) return;
		//count++;
		int I_ab = data[0][0];
		if(I_ab == 0) return;
		//cout<<"g_fuc_key"<<key<<endl;
		double outv = delta;
		int size_a = data[1].size();
		int size_b = data[2].size();
		if(size_a == 0 || size_b == 0) return;
		vector<string> list;
		//list.push_back(" ");
		for(vector<int>::const_iterator it_a = data[1].begin(); it_a != data[1].end(); it_a++){
			for(vector<int>::const_iterator it_b = data[2].begin(); it_b != data[2].end(); it_b++){
				int a = *it_a;
				int b = *it_b;
				//cout<<"a:"<<a<<"    "<<"b:"<<b<<endl;
				string key_a = to_string(a);
				string key_b = to_string(b);
				string key;
				if(a == b) continue;
				if(a < b){
					key = key_a + "_" + key_b;
				}else{
					key = key_b + "_" + key_a;
				}
				int pos = 1;
				for(vector<string>::const_iterator it = list.begin(); it != list.end(); it++){
					string k = *it;
					if(k == key){
						pos = 0;
						break;
					}
				}
				if(pos == 1){
					list.push_back(key);
					output->push_back(make_pair(key, outv));
					// cout<<"key: "<<key<<endl;
				}
			}
		}
		// cout<<"g_fun"<<endl;
	}

	const double& default_v() const{
		return zero;
	}
};

static int Simrank(ConfigData& conf){
	MaiterKernel<string, double, vector<vector<int> > >* kernel = new MaiterKernel<string, double,
			vector<vector<int> > >(
			conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
			new Sharders::Mod_str,
			new Simrankiterate,
			//new SUM);
			new TermCheckers<string, double>::Sum);

	kernel->registerMaiter();

	if(!StartWorker(conf)){
		Master m(conf);
		m.run_maiter(kernel);
	}

	delete kernel;
	return 0;
}

REGISTER_RUNNER(Simrank);

