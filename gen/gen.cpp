/*
 * main.cpp
 *
 *  Created on: Jan 11, 2016
 *      Author: tzhou
 *  Modified on Mar 17, 2017
 *      Add weight and more options
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <sys/stat.h>
#include <random>
#include <functional>
#include "pl-dis.hpp"

using namespace std;

// ------ unweighted ------

vector<vector<int> > gen_uw(const int nPart, const int nNode, const unsigned long seed,
	function<unsigned(mt19937&)> rngDeg)
{
	vector<vector<int> > g(nNode);
	mt19937 gen(seed);
	auto deg_gen=bind(rngDeg,gen);
	uniform_int_distribution<unsigned> n_dis(0,nNode-1);
	auto dst_gen=[&](){ return n_dis(gen); };
	for(int i=0;i<nNode;++i){
		int m=deg_gen();
		for(int j=0;j<m;++j){
			g[i].push_back(dst_gen());
		}
		sort(g[i].begin(),g[i].end());
		g[i].erase(unique(g[i].begin(),g[i].end()),g[i].end());
	}
	return g;
}

int dump_uw(const vector<vector<int> >& g, const int nPart, const string& outDir){
	mkdir(outDir.c_str(),0755);
	vector<ofstream*> fout;
	for(int i=0;i<nPart;++i){
		fout.push_back(new ofstream(outDir+"/part"+to_string(i)));
		if(!fout.back()->is_open()){
			cerr<<"failed in opening file: "<<outDir+"/part"+to_string(i)<<endl;
			return i;
		}
	}
	for(size_t i=0;i<g.size();++i){
		int idx=i%nPart;
		*fout[idx]<<i<<"\t";
		for(int v : g[i])
			*fout[idx]<<v<<" ";
		*fout[idx]<<"\n";
	}
	for(size_t i=0;i<fout.size();++i){
		delete fout[i];
	}
	return nPart;
}

// ------ weighted ------

vector<vector<pair<int,double>> > gen_w(const int nPart, const int nNode, const unsigned long seed,
	function<unsigned(mt19937&)> rngDeg, function<double(mt19937&)> rngWgt)
{
	vector<vector<pair<int,double>> > g(nNode);
	mt19937 gen(seed);
	auto deg_gen=bind(rngDeg,gen);
	uniform_int_distribution<unsigned> n_dis(0,nNode-1);
	auto dst_gen=[&](){ return n_dis(gen); };
	auto wgt_gen=bind(rngWgt,gen);
	for(int i=0;i<nNode;++i){
		int m=deg_gen();
		for(int j=0;j<m;++j){
			g[i].emplace_back(dst_gen(),wgt_gen());
		}
		sort(g[i].begin(),g[i].end(),[](const pair<int,double>& lth, const pair<int,double>& rth){
			return lth.first<rth.first;
		});
		auto it=unique(g[i].begin(),g[i].end(),[](const pair<int,double>& lth, const pair<int,double>& rth){
			return lth.first==rth.first;
		});
		g[i].erase(it,g[i].end());
	}
	return g;
}

int dump_w(const vector<vector<pair<int,double>> >& g, const int nPart, const string& outDir){
	mkdir(outDir.c_str(),0755);
	vector<ofstream*> fout;
	for(int i=0;i<nPart;++i){
		fout.push_back(new ofstream(outDir+"/part"+to_string(i)));
		if(!fout.back()->is_open()){
			cerr<<"failed in opening file: "<<outDir+"/part"+to_string(i)<<endl;
			return i;
		}
	}
	for(size_t i=0;i<g.size();++i){
		int idx=i%nPart;
		*fout[idx]<<i<<"\t";
		for(pair<int,double> vw : g[i])
			*fout[idx]<<vw.first<<","<<vw.second<<" ";
		*fout[idx]<<"\n";
	}
	for(size_t i=0;i<fout.size();++i){
		delete fout[i];
	}
	return nPart;
}

// ------ main ------

struct Option{
	int nPart, nNode;
	string dist;
	double alpha;// for power-law distribution
	string weight;
	double wmin,wmax;
	string outDir;
	unsigned long seed;

	void parse(int argc, char* argv[]);
private:
	bool setDist(string& method);
	bool setWeight(string& method);
};
void Option::parse(int argc, char* argv[]){
	nPart=stoi(string(argv[1]));
	nNode=stoi(string(argv[2]));
	outDir=".";
	if(argc>3)
		outDir=argv[3];
	string distMethod="pl:2.3";
	if(argc>4)
		distMethod=argv[4];
	string weightMethod="no";
	if(argc>5)
		weightMethod=argv[5];
	seed=1535345;
	if(argc>=7)
		seed=stoul(string(argv[6]));
	// check distribution
	if(!setDist(distMethod))
		throw invalid_argument("unsupported degree distribution: "+distMethod);
	if(!setWeight(weightMethod))
		throw invalid_argument("unsupported weight distribution: "+weightMethod);
}
bool Option::setDist(string& method){
	if(method=="uni"){
		alpha=2.0;
		dist="uni";
	}else if(method.substr(0,3)=="pl:"){
		alpha=stod(method.substr(3));
		dist="pl";
	}else{
		return false;
	}
	return true;
}
bool Option::setWeight(string& method){
	if(method=="no"){
		weight="no";
	}else if(method.substr(0,7)=="weight:"){
		weight="weight";
		size_t p=method.find(',',7);
		wmin=stod(method.substr(7,p-7));
		wmax=stod(method.substr(p+1));
	}else{
		return false;
	}
	return true;
}

int main(int argc, char* argv[]){
	if(argc<3 || argc>7){
		cerr<<"Wrong usage.\nUsage: \"gen #parts #nodes [out-dir] [deg-dist:<param>] [weight:<min>,<max>] [random-seed]\""<<endl;
		cerr<<"  [deg-dist]: the distribution of degrees. Support: uni (uniform), pl:<alpha> (power-law with alpha). Ddefault: pl:2.3\n"
			"  [weight]: the weight distribution. If unweighted graph is needed, use \"no\" here. Default: no\n"
			"  [random-seed]: seed for random numbers. Default: 1535345\n"
			"i.e.: ./gen 2 100 pl:2.6 no out 123456\n"
			"i.e.: ./gen 2 100 uni weight:0,1 outW\n"<<endl;
		return 1;
	}
	Option opt;
	try{
		opt.parse(argc, argv);
	}catch(exception& e){
		cerr<<e.what()<<endl;
		return 2;
	}
	ios_base::sync_with_stdio(false);
	cout<<"generating with "<<opt.nNode<<" nodes, "<<opt.nPart<<" parts, in folder: "<<opt.outDir<<endl;
	cout<<"degree distribution: "<<opt.dist<<", random seed: "<<opt.seed<<endl;

	function<unsigned(mt19937&)> rngDeg;
	uniform_int_distribution<int> uni_dis(0,opt.nNode-1);
	power_law_distribution<unsigned> pl_dis(1, opt.alpha);
	if(opt.dist=="uni"){
		rngDeg=bind(uni_dis,placeholders::_1);
	}else{
		rngDeg=[&](mt19937& m){ return min<unsigned>(pl_dis(m), opt.nNode); };
	}

	int n;
	if(opt.weight=="no"){
		vector<vector<int> > g=gen_uw(opt.nPart,opt.nNode,opt.seed,rngDeg);
		cout<<"dumping"<<endl;
		n=dump_uw(g,opt.nPart,opt.outDir);
	}else{
		uniform_real_distribution<double> uni_dis(opt.wmin, opt.wmax);
		function<double(mt19937&)> rngWgt=bind(uni_dis,placeholders::_1);
		vector<vector<pair<int,double>> > g=gen_w(opt.nPart,opt.nNode,opt.seed,rngDeg,rngWgt);
		cout<<"dumping"<<endl;
		n=dump_w(g,opt.nPart,opt.outDir);
	}
	cout<<"success "<<n<<" files. fail "<<opt.nPart-n<<" files."<<endl;
	return 0;
}

