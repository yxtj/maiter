#include <iostream>
#include <string>
#include <fstream>
#include <queue>
#include <algorithm>
#include <functional>
#include <chrono>

#include "common.h"

using namespace std;

vector<float> cal_wp_spfa(vector<vector<Edge>>& g, int source){
	static constexpr float inf = numeric_limits<float>::infinity();
	size_t n=g.size();
	vector<float> res(n, 0.0f);
	vector<bool> inque(n, false);
	deque<int> rque;
	int f=0, l=0, nf=0;
	float total=0;
	rque.push_back(source);
	res[source]=inf;
	++nf;
	while(!rque.empty()){
		 // standard version without any optimization
		int t=rque.front();
		rque.pop_front();
		inque[t]=false;
		for(auto& e: g[t]){
			float temp=min(res[t], e.weight);
			if(temp>res[e.node]){
				res[e.node]=temp;
				if(!inque[e.node]){
					rque.push_back(e.node);
					inque[e.node]=true;
				}
			}
		}
		/*
		int t=rque.front();
		rque.pop_front();
		while(res[t]*nf>total){	// LLL
			rque.push_back(t);
			t=rque.front();
			rque.pop_front();
		}
		float width=res[t];
		inque[t]=false;
		total-=width;
		--nf;
		for(auto& e: g[t]){
			float temp=min(width, e.weight);
			if(temp>res[e.node]){
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
		}*/
	}
	return res;
}

vector<float> cal_wp(vector<vector<Edge>>& g, int source, const string& method){
	return cal_wp_spfa(g, source);
}

// track source of each node to find out the critical edges

vector<pair<int,int>> cal_critical_edges(const vector<vector<Edge>>& g, const vector<float>& wp, const int source){
	size_t n=g.size();
	vector<pair<int,int>> res;
	queue<int> que;
	vector<bool> used(n, false);
	que.push(source);
	used[source]=true;
	while(!que.empty()){
		int src=que.front();
		que.pop();
		for(const Edge& e : g[src]){
			if(wp[e.node] == e.weight){
				res.emplace_back(src, e.node);
			}
			if(!used[e.node]){
				que.push(e.node);
				used[e.node]=true;
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

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate Singe Source Widest Path."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [source] [opt-critical-edge] [algorithm]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used\n"
			<<"  [source]: (=0) the source node in the graph\n"
			<<"  [opt-critical-edge]: (=0) whether to output the critical edge in the shortest paths (file name prefix: cedge)\n"
			<<"  [algorithm]: (=spfa) the algorithm for SSWP. Supports: spfa"
			<<endl;
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
	string method="spfa";
	if(argc>6){
		method=argv[6];
	}
	if(method!="spfa"){
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
	cout<<"calculating SSWP"<<endl;
	start_t = chrono::system_clock::now();
	vector<float> wp = cal_wp(g, source, method);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// dump
	cout<<"dumping SSWP"<<endl;
	start_t = chrono::system_clock::now();
	vector<string> fnout;
	for(int i=0;i<parts;++i){
		fnout.push_back(outprefix+"/part-"+to_string(i));
	}
	if(!dump(fnout, wp)){
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
	vector<pair<int, int>> cedges = cal_critical_edges(g, wp, source);
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

	return 0;
}
