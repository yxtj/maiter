#include <iostream>
#include <string>
#include <fstream>
#include <queue>
#include <algorithm>
#include <functional>
#include <chrono>

using namespace std;

struct Edge{
	int node;
	float weight;
};

bool operator<(const Edge &a,const Edge &b){
	return a.weight < b.weight;
}
bool operator>(const Edge &a,const Edge &b){
	return a.weight > b.weight;
}

vector<vector<Edge>> load_graph(const string& fn){
	vector<vector<Edge>> res;
	ifstream fin(fn);
	if(!fin){
		cerr<<"Error: cannot open input file: "<<fn<<endl;
		exit(0);
	}
	string line;
	while(getline(fin, line)){
		if(line.size()<2)
			continue;
		size_t pos = line.find('\t');
		int k = stoi(line.substr(0, pos));
		++pos;
		vector<Edge> temp;
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			size_t cut = line.find(',', pos + 1);
			int node=stoi(line.substr(pos, cut - pos));
			float weight=stof(line.substr(cut + 1, spacepos - cut - 1));
			Edge e{node, weight};
			temp.push_back(e);
			pos = spacepos + 1;
		}
		if(res.size() < k)	// k starts from 0
			res.resize(k);	// fill the empty holes
		res.push_back(move(temp));
	}
	return res;
}

vector<float> cal_sp_dijkstra(vector<vector<Edge>>& g, int source){
	size_t n=g.size();
	vector<float> res(n, numeric_limits<float>::max());
	vector<bool> found(n, false);
	priority_queue<Edge, vector<Edge>, greater<Edge>> heap; // Edge is re-used as struct <dis, node>
	Edge e{source, 0.0f};
	heap.push(e);
	res[source]=0.0f;
	size_t nf=0;
	while(nf<n && !heap.empty()){
		Edge top;
		do{
			top=heap.top();
			heap.pop();
		}while(found[top.node]);	// quickly skip found nodes
		++nf;
		found[top.node]=true;
		float dis=top.weight;
		res[top.node]=dis;
		for(auto& e : g[top.node]){
			float temp=dis+e.weight;
			if(temp<res[e.node]){
				Edge xx{e.node, temp};
				res[e.node]=temp;
				heap.push(xx);
			}
		}
	}
	return res;
}

vector<float> cal_sp_spfa(vector<vector<Edge>>& g, int source){
	size_t n=g.size();
	vector<float> res(n, numeric_limits<float>::max());
	vector<bool> inque(n, false);
	deque<int> rque;
	int f=0, l=0, nf=0;
	float total=0;
	rque.push_back(source);
	res[source]=0;
	++nf;
	while(!rque.empty()){
		/* // standard version without any optimization
		int t=rque.front();
		rque.pop_front();
		inque[t]=false;
		for(auto& e: g[t]){
			float temp=res[t]+e.weight;
			if(temp<res[e.node]){
				res[e.node]=temp;
				if(!inque[e.node]){
					rque.push_back(e.node);
					inque[e.node]=true;
				}
			}
		}*/
		int t=rque.front();
		rque.pop_front();
		while(res[t]*nf>total){	// LLL
			rque.push_back(t);
			t=rque.front();
			rque.pop_front();
		}
		float dis=res[t];
		inque[t]=false;
		total-=dis;
		--nf;
		for(auto& e: g[t]){
			float temp=dis+e.weight;
			if(temp<res[e.node]){
				res[e.node]=temp;
				if(!inque[e.node]){
					if(temp*nf<total)	// SLF
						rque.push_front(e.node);
					else	// LLL
						rque.push_back(e.node);
					inque[e.node]=true;
					total+=temp;
					++nf;
				}
			}
		}
	}
	return res;
}

vector<float> cal_sp(vector<vector<Edge>>& g, int source, const string& method){
	if(method=="dijkstra")
		return cal_sp_dijkstra(g, source);
	else //if(method=="spfa")
		return cal_sp_spfa(g, source);
}

bool dump(const string& fn, const vector<float>& res){
	ofstream fout(fn);
	if(!fout)
		return false;
	size_t size=res.size();
	for(size_t i=0;i<size;++i){
		fout<<i<<"\t0:"<<res[i]<<"\n";
	}
	return true;
}

int main(int argc, char* argv[]){
	if(argc<3){
		cerr<<"Calculate SSSP."<<endl;
		cerr<<"Usage: <in-file> <out-file> [source] [algorithm]\n"
			<<"  [source]: (=0) the source node in the graph\n"
			<<"  [algorithm]: (=dijkstra) the algorithm for SSSP. Supports: dijkstra, spfa"<<endl;
		return 1;
	}
	string infile=argv[1];
	string outfile=argv[2];
	int source=0;
	if(argc>3){
		source=stoi(argv[3]);
	}
	string method="dijkstra";
	if(argc>4){
		method=argv[4];
	}
	if(method!="dijkstra" && method!="spfa"){
		cerr<<"Error: unsupported algorithm: "<<method<<endl;
		return 2;
	}
	chrono::time_point<std::chrono::system_clock> start_t;
	chrono::duration<double> elapsed;
	
	cout<<"loading graph"<<endl;
    start_t = chrono::system_clock::now();
	vector<vector<Edge>> g = load_graph(infile);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  load "<<g.size()<<" nodes in "<<elapsed.count()<<" seconds"<<endl;
	
	cout<<"processing"<<endl;
	start_t = chrono::system_clock::now();
	vector<float> sp = cal_sp(g, source, method);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	cout<<"dumping"<<endl;
	start_t = chrono::system_clock::now();
	if(!dump(outfile, sp)){
		cerr<<"Error: cannot write to given fine"<<endl;
		return 3;
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	return 0;
}