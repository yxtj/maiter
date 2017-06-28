#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <cmath>
#include <chrono>

#include "common.h"

using namespace std;

vector<float> cal_pr(const vector<vector<int> >& g, const float damp, const int maxIter, const double epsilon) {
	size_t n=g.size();

	vector<float> res(n, 1-damp);
	vector<float> old;
	
	int iter = 0;
	double sum=n*static_cast<double>(1-damp);
	double oldsum=0;
	while(++iter < maxIter && abs(sum-oldsum) > epsilon){
		old.swap(res); // old=move(res);
		oldsum=sum;
		res.assign(n, 1-damp);
		sum=n*static_cast<double>(1-damp);
		for(size_t i=0;i<n;++i){
			const vector<int>& line=g[i];
			float out=damp*old[i]/line.size();
			for(int dst : line){
				res[dst]+=out;
			}
			sum+=damp*old[i];
		}
	}
	cout<<"  iterations: "<<iter<<"\tdifference: "<<sum-oldsum<<endl;
	return res;
}

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate PageRank."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [dump-factor] [max-iter] [epsilon]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used\n"
			<<"  [damp-factor]: (=0.8) the damping factor (the portion of values transitted) for PageRank\n"
			<<"  [max-iter]: (=inf) the maximum number of iterations until termination\n"
			<<"  [epsilon]: (=1e-6) the minimum difference between consecutive iterations for termination check"
			<<endl;
		return 1;
	}
	int parts=stoi(argv[1]);
	string inprefix=argv[2];
	string outprefix=argv[3];
	float damp=0.8;
	if(argc>4)
		damp=stof(argv[4]);
	int maxIter=numeric_limits<int>::max();
	if(argc>5)
		maxIter=stoi(argv[5]);
	double termDiff=1e-6;
	if(argc>6)
		termDiff=stod(argv[6]);
	
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
	vector<float> res = cal_pr(g, damp, maxIter, termDiff);
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	
	// dump
	cout<<"dumping"<<endl;
	start_t = chrono::system_clock::now();
	vector<string> fnout;
	for(int i=0;i<parts;++i){
		fnout.push_back(outprefix+"/part-"+to_string(i));
	}
	if(!dump(fnout, res)){
		cerr<<"Error: cannot write to given file(s)"<<endl;
		return 4;
	}
    elapsed = chrono::system_clock::now()-start_t;
	cout<<"  finished in "<<elapsed.count()<<" seconds"<<endl;
	return 0;
}
