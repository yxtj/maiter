#include <iostream>
#include <string>
#include <fstream>
#include <queue>
#include <algorithm>
#include <functional>
#include <chrono>

#include "common.h"

using namespace std;

vector<float> cal_sp_dijkstra(vector<vector<Edge>>& g, int source){
	size_t n=g.size();
	vector<float> res(n, numeric_limits<float>::infinity());
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
	vector<float> res(n, numeric_limits<float>::infinity());
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

// track source of each node to find out the critical edges

vector<pair<int,int>> cal_critical_edges(const vector<vector<Edge>>& g, const vector<float>& sp, const int source){
	size_t n=g.size();
	vector<pair<int,int>> res;
	queue<int> que;
	que.push(source);
	while(!que.empty()){
		int src=que.front();
		que.pop();
		float sd=sp[src];
		for(const Edge& e : g[src]){
			if(sp[e.node] == sd + e.weight){
				res.emplace_back(src, e.node);
				que.push(e.node);
			}
		}
	}
	return res;
}

bool dump_cedge(const vector<string>& fncedge, const vector<pair<int, int>>& cedges){
	size_t parts=fncedge.size();
	vector<ofstream*> fouts;
	for(size_t i=0;i<parts;++i){
		ofstream* pf=new ofstream(fncedge[i]);
		if(!pf || !pf->is_open())
			return false;
		fouts.push_back(pf);
	}
	size_t size=cedges.size();
	for(size_t i=0;i<size;++i){
		int src, dst;
		tie(src, dst)=cedges[i];
		(*fouts[src%parts])<<src<<" "<<dst<<"\n";
	}
	for(size_t i=0;i<parts;++i)
		delete fouts[i];
	return true;
}

// for checking
void build_sp_with_critical_edges(const vector<vector<Edge>>& g, const vector<pair<int, int>>& cedges, string fn){
	size_t n=g.size();
	vector<float> sd(n, numeric_limits<float>::infinity());
	queue<int> que;
	sd[0]=0;
	que.push(0);
	while(!que.empty()){
		int t=que.front();
		que.pop();
		auto it=find_if(cedges.begin(), cedges.end(), [&](const pair<int,int>& p){
			return p.first==t;
		});
		while(it!=cedges.end()){
			int dst=it->second;
			que.push(dst);
			auto jt=find_if(g[t].begin(), g[t].end(), [&](const Edge& e){
				return e.node==dst;
			});
			sd[dst]=sd[t]+jt->weight;
			it=find_if(++it, cedges.end(), [&](const pair<int,int>& p){
				return p.first==t;
			});
		}
	}
	ofstream fout(fn);
	for(size_t i=0;i<n;++i){
		fout<<i<<"\t0:"<<sd[i]<<"\n";
	}
}

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate SSSP."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [source] [opt-critical-edge] [algorithm]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used\n"
			<<"  [source]: (=0) the source node in the graph\n"
			<<"  [opt-critical-edge]: (=0) whether to output the critical edge in the shortest paths (file name prefix: cedge)\n"
			<<"  [algorithm]: (=dijkstra) the algorithm for SSSP. Supports: dijkstra, spfa"<<endl;
		return 1;
	}
	int parts=stoi(argv[1]);
	string inprefix=argv[2];
	string outprefix=argv[3];
	int source=0;
	if(argc>4){
		source=stoi(argv[4]);
	}
	bool get_cedge=false;
	if(argc>5){
		string opt=argv[5];
		if(opt=="1" || opt=="y" || opt=="t" || opt=="yes" || opt=="true")
			get_cedge=true;
	}
	string method="dijkstra";
	if(argc>6){
		method=argv[6];
	}
	if(method!="dijkstra" && method!="spfa"){
		cerr<<"Error: unsupported algorithm: "<<method<<endl;
		return 2;
	}
	chrono::time_point<std::chrono::system_clock> start_t;
	chrono::duration<double> elapsed;
	
	// load
	cout<<"loading graph"<<endl;
    start_t = chrono::system_clock::now();
	vector<vector<Edge>> g;
	for(int i=0;i<parts;++i){
		string fn=inprefix+"/part"+to_string(i);
		cout<<"  loading "<<fn<<endl;
		if(!load_graph_weight(g, fn)){
			cerr<<"Error: cannot open input file: "<<fn<<endl;
			return 3;
		}
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  load "<<g.size()<<" nodes in "<<elapsed.count()<<" seconds"<<endl;
	
	// calculate
	cout<<"calculating SSSP"<<endl;
	start_t = chrono::system_clock::now();
	vector<float> sp = cal_sp(g, source, method);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// dump
	cout<<"dumping SSSP"<<endl;
	start_t = chrono::system_clock::now();
	vector<string> fnout;
	for(int i=0;i<parts;++i){
		fnout.push_back(outprefix+"/part-"+to_string(i));
	}
	if(!dump(fnout, sp)){
		cerr<<"Error: cannot write to given file(s)"<<endl;
		return 4;
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// critical edge
	if(!get_cedge)
		return 0;
	
	cout<<"calculating critical edges"<<endl;
	start_t = chrono::system_clock::now();
	vector<pair<int, int>> cedges = cal_critical_edges(g, sp, source);
	elapsed = chrono::system_clock::now()-start_t;
	cout<<"  found "<<cedges.size()<<" critical edges"<<endl;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	cout<<"dumping critical edges"<<endl;
	start_t = chrono::system_clock::now();
	vector<string> fncedge;
	for(int i=0;i<parts;++i)
		fncedge.push_back(outprefix+"/cedge-"+to_string(i));
	if(!dump_cedge(fncedge, cedges)){
		cerr<<"Error: cannot write to given file(s)"<<endl;
		return 5;
	}
	elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;

	//build_sp_with_critical_edges(g, cedges, outprefix+"/xx.txt");
	
	return 0;
}
