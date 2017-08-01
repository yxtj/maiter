#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <cmath>
#include <chrono>

#include "common.h"

using namespace std;

vector<float> cal_mc(const vector<vector<Edge>>& g, const int maxIter, const double epsilon) {
	size_t n=g.size();

	vector<float> res(n, 1.0/n);
	vector<float> old;
	
	int iter = 0;
	// because the summation is always 1, we use sum of square here
	double sum=1.0/n; // = n* (1/n)^2
	double oldsum=0;
	while(++iter < maxIter && abs(sum-oldsum) > epsilon){
		old.swap(res); // old=move(res);
		oldsum=sum;
		res.assign(n, 0);
		sum=0;
		for(size_t i=0;i<n;++i){
			const float v = old[i];
			for(const Edge& e: g[i]){
				float t=v*e.weight;
				res[e.node]+=t;
				sum+=t*t;
			}
		}
	}
	cout<<"  iterations: "<<iter<<"\tdifference: "<<sum-oldsum<<endl;
	return res;
}

int main(int argc, char* argv[]){
	if(argc<=3){
		cerr<<"Calculate the stationary distribution of a Markov Chain."<<endl;
		cerr<<"Usage: <#parts> <in-folder> <out-folder> [normalize] [max-iter] [epsilon]\n"
			<<"  <in-folder>: input file prefix, file name: 'part<id>' is automatically used\n"
			<<"  <out-folder>: output file prefix, file name 'part-<id>' is automatically used\n"
			<<"  [normalize]: (=0) whether to output the normalized result, otherwise output <n-node> * <real-result> \n"
			<<"  [max-iter]: (=inf) the maximum number of iterations until termination\n"
			<<"  [epsilon]: (=1e-9) the minimum difference between consecutive iterations for termination check"
			<<endl;
		return 1;
	}
	int parts=stoi(argv[1]);
	string inprefix=argv[2];
	string outprefix=argv[3];
	bool normalize=true;
	if(argc>4){
		string opt(argv[4]); // cannot use argv[4]=="x" (this is comparing the pointer)
		normalize=(opt=="1" || opt=="y" || opt=="t" || opt=="yes" || opt=="true");
	}
	int maxIter=numeric_limits<int>::max();
	if(argc>5)
		maxIter=stoi(argv[5]);
	double termDiff=1e-9;
	if(argc>6)
		termDiff=stod(argv[6]);
	
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

	// normalize transition distribution
	cout<<"normalizing the input transition distribution"<<endl;
	for(auto& line: g){
		float sum=0.0f;
		for(Edge& e : line)
			sum+=e.weight;
		for(Edge& e : line)
			e.weight/=sum;
	}

	// calculate
	cout<<"calculating"<<endl;
	start_t = chrono::system_clock::now();
	vector<float> res = cal_mc(g, maxIter, termDiff);
	if(!normalize){
		int n=g.size();
		for(auto& v : res)
			v*=n;
	}
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
