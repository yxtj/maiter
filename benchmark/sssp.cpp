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

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate SSSP."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [source] [algorithm]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used\n"
			<<"  [source]: (=0) the source node in the graph\n"
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
	string method="dijkstra";
	if(argc>5){
		method=argv[5];
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
	cout<<"calculating"<<endl;
	start_t = chrono::system_clock::now();
	vector<float> sp = cal_sp(g, source, method);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// dump
	cout<<"dumping"<<endl;
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
	return 0;
}
