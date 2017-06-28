#include <iostream>
#include <string>
#include <fstream>
#include <chrono>

#include "common.h"

using namespace std;

// return maximum label in its brunch
void dfs_set(const int p, vector<int>& gid, const vector<vector<int> >& g){
	int& lbl=gid[p]; // reference to gid[p]
	auto itend=g[p].rend();
	for(auto it=g[p].rbegin(); it!=itend; ++it){ // get the larger label first
		int dst=*it;
		if(lbl != gid[dst]){
			lbl=max(lbl, gid[dst]);
			gid[dst]=lbl;
			dfs_set(dst, gid, g);
		}
	}
}

vector<int> cal_cc(const vector<vector<int> >& g) {
	size_t n=g.size();
	// initiate gid as nid
	vector<int> gid;
	gid.reserve(n);
	for(int i=0; i<n; ++i){
		gid.push_back(i);
	}
	
	int iter=0;
	bool changed=true;
	while(changed){
		++iter;
		changed=false;
		for(int i=n-1;i>=0;--i){ // get the larger label first
			int old = gid[i];
			dfs_set(i, gid, g);
			//cout<<i<<"\t"<<old<<" - "<<gid[i]<<endl;
			changed |= old != gid[i];
		}
	}
	cout<<"  iterations: "<<iter<<endl;
	return gid;
}

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate Connected Component."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [source] [algorithm]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used"
			<<endl;
		return 1;
	}
	int parts=stoi(argv[1]);
	string inprefix=argv[2];
	string outprefix=argv[3];
	
	chrono::time_point<std::chrono::system_clock> start_t;
	chrono::duration<double> elapsed;
	
	// load
	cout<<"loading graph"<<endl;
    start_t = chrono::system_clock::now();
	vector<vector<int>> g;
	for(int i=0;i<parts;++i){
		string fn=inprefix+"/part"+to_string(i);
		cout<<"  loading "<<fn<<endl;
		if(!load_graph_unweight(g, fn)){
			cerr<<"Error: cannot open input file: "<<fn<<endl;
			return 3;
		}
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  load "<<g.size()<<" nodes in "<<elapsed.count()<<" seconds"<<endl;

	// calculate
	cout<<"calculating"<<endl;
	start_t = chrono::system_clock::now();
	vector<int> cc = cal_cc(g);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// dump
	cout<<"dumping"<<endl;
	start_t = chrono::system_clock::now();
	vector<string> fnout;
	for(int i=0;i<parts;++i){
		fnout.push_back(outprefix+"/part-"+to_string(i));
	}
	if(!dump(fnout, cc)){
		cerr<<"Error: cannot write to given file(s)"<<endl;
		return 4;
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	return 0;
}
