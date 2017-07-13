/*
 * deltaGen.cpp
 *
 *  Created on: Jan 11, 2016
 *      Author: tzhou
 *  Modified on Mar 17, 2017
 *      Add weight and more options
 *  Modified on April 23, 2017 by GZ
 *		generate delta graph files
 */

#include <iostream>
#include <iomanip>
#include <fstream>
#include <vector>
#include <algorithm>
#include <random>
#include <functional>
#include <unordered_set>
#include "common.h"

using namespace std;

// ---- load the graph data and generate the delta file

struct ModifyThreshold{
	double trv;	// trivial (not change)
	double add;	// add an edge
	double rmv; // remove an edge
	double inc; // increase the weight of an edge
	double dec; // decrease the weight of an edge
};

struct ModifyEdges{
	vector<EdgeW> addSet;
	vector<EdgeW> rmvSet;
	vector<EdgeW> incSet;
	vector<EdgeW> decSet;
	
	uniform_real_distribution<double> rnd_prob;
	uniform_int_distribution<int> rnd_node;
	uniform_real_distribution<float> rnd_weight;
	
	ModifyEdges(uniform_real_distribution<double>& rnd_prob, uniform_int_distribution<int>& rnd_node,
		uniform_real_distribution<float>& rnd_weight);
	int set(const ModifyThreshold& threshold, const int src, const Link& link,
		const vector<Link>& edges, const int maxV, mt19937& gen);
	
};

ModifyEdges::ModifyEdges(uniform_real_distribution<double>& rnd_prob,
	uniform_int_distribution<int>& rnd_node, uniform_real_distribution<float>& rnd_weight)
	: rnd_prob(rnd_prob), rnd_node(rnd_node), rnd_weight(rnd_weight)
{}
		
int ModifyEdges::set(const ModifyThreshold& threshold, const int src, const Link& link,
	const vector<Link>& edges, const int maxV, mt19937& gen)
{
	double r = rnd_prob(gen);
	if(r < threshold.trv){
		return 0;
	}else if(r < threshold.add){
		int rpt = 10;
		int newV;
		do{
			newV = rnd_node(gen) % maxV;
		}while(rpt-- > 0 && lower_bound(edges.begin(), edges.end(), maxV,
			[&](const Link& l, const int v){return l.node<v;}) != edges.end());
		if(rpt > 0){
			EdgeW e{ src, newV, rnd_weight(gen) };
			addSet.push_back(e);
		}
		return 1;
	}else if(r < threshold.rmv){
		EdgeW e{ src, link.node, link.weight };
		rmvSet.push_back(e);
		return 2;
	}else if(r < threshold.inc){
		EdgeW e{ src, link.node, link.weight*(1 + rnd_weight(gen)) };
		incSet.push_back(e);
		return 3;
	}else if(r < threshold.dec){
		EdgeW e{ src, link.node, link.weight*rnd_weight(gen) };
		decSet.push_back(e);
		return 4;
	}
	return 5;
}

void dumpChangeOneSet(ofstream& fout, const vector<EdgeW>& edgeSet, char type, bool bidir){
	if(!bidir){
		for(const EdgeW& e : edgeSet){
			fout << type << " " << e.src << "," << e.dst << "," << e.weight << "\n";
		}
	}else{
		for(const EdgeW& e : edgeSet){
			fout << type << " " << e.src << "," << e.dst << "," << e.weight << "\n";
			fout << type << " " << e.dst << "," << e.src << "," << e.weight << "\n";
		}
	}
}
		
bool changeGraph(const string& graphFolder, const string& cedgeFolder, const string& deltaPrefix,
		const int nPart, const int seed,
		const double deltaRatio, const double crtRatio, const double goodRatio, const double ewRatio, const bool bidir)
{
	vector<ifstream*> fin;
	vector<ifstream*> fce;
	vector<ofstream*> fout;
	for(int i = 0; i < nPart; ++i){
		fin.push_back(new ifstream(graphFolder + "/part" + to_string(i)));
		if(!fin.back()->is_open()){
			cerr << "failed in opening input file: " << graphFolder + "/part" + to_string(i) << endl;
			return false;
		}
		fce.push_back(new ifstream(cedgeFolder + "/cedge-" + to_string(i)));
		if(!fce.back()->is_open()){
			cerr << "failed in opening cedge file: " << cedgeFolder + "/cedge-" + to_string(i) << endl;
			return false;
		}
		fout.push_back(new ofstream(deltaPrefix + "-" + to_string(i)));
		if(!fout.back()->is_open()){
			cerr << "failed in opening output file: " << deltaPrefix + "-" + to_string(i) << endl;
			return false;
		}
	}

	mt19937 gen(seed);
	uniform_real_distribution<double> rnd_prob(0.0, 1.0);
	uniform_int_distribution<int> rnd_node; // 0 to numeric_limits<int>::max()
	uniform_real_distribution<float> rnd_weight(0, 1);

	int totalV = 0, totalE = 0, totalC = 0;
	int maxV = 0;
	int cRmvCnt = 0, cIncCnt = 0, cDecCnt = 0;
	int addCnt = 0, rmvCnt = 0, incCnt = 0, decCnt = 0;
	
	for(int i=0;i<nPart;++i){
		cout<<"  On part "<<i<<"...";
		cout.flush();
		ModifyEdges modifiedSet(rnd_prob, rnd_node, rnd_weight);
		size_t nc, nv, ne;
		vector<pair<int,int>> cedges = load_critical_edges(*fce[i]);
		nc = cedges.size();
		unordered_map<int, vector<Link>> edges;
		tie(edges, ne) = load_graph_weight_one(*fin[i]);
		nv = edges.size();
		delete fce[i];
		delete fin[i];
		double maxCrtRatio = deltaRatio*ne/nc;	// the maximum allowed crtRatio by the current deltaRatio
		double minDltRatio = crtRatio*nc/ne;	// the minimum required deltaRatio by the current crtRatio
		
		cout<<" Found: vertex/edges/critical: "<<nv<<"/"<<ne<<"/"<<nc
			<<", min-DR="<<minDltRatio<<", max-CR="<<maxCrtRatio<<endl;
	
		if(minDltRatio > deltaRatio){
			cerr<<"  Warning: on part "<<i<<" crtRatio and deltaRatio do not match."<<endl;
		}
		double cr = min(maxCrtRatio, crtRatio);	// delta ratio for critical edges
		double dr = (deltaRatio*ne - cr*nc) / (ne - nc);	// delta ratio for non-critical edges
		
		ModifyThreshold thresholdCE, thresholdNE; //{ trivial, addTh, rmvTh, incTh, decTh };
		// critical edge
		thresholdCE.trv = (1 - cr);
		thresholdCE.add = thresholdCE.trv;
		thresholdCE.rmv = thresholdCE.add + cr * ewRatio *  (1-goodRatio);
		thresholdCE.inc = thresholdCE.rmv + cr *(1-ewRatio)*(1-goodRatio);
		thresholdCE.dec = thresholdCE.inc + cr *goodRatio;
		// non-critical edge
		thresholdNE.trv = (1 - dr);
		thresholdNE.add = thresholdNE.trv + dr * ewRatio *  goodRatio;
		thresholdNE.rmv = thresholdNE.add + dr * ewRatio *  (1-goodRatio);
		thresholdNE.inc = thresholdNE.rmv + dr *(1-ewRatio)*(1-goodRatio);
		thresholdNE.dec = thresholdNE.inc + dr *(1-ewRatio)*goodRatio;
		//cout<<thresholdCE.dec<<"\t"<<thresholdNE.dec<<endl;
		
		for(auto& p : edges){
			int k = p.first;
			// [pFirst, pLast) are the critical edges start with k
			auto pFirst = lower_bound(cedges.begin(), cedges.end(), k, 
				[](const pair<int,int>& a, const int b){
					return a.first < b;
			});
			auto pLast = pFirst;
			while(pLast != cedges.end() && pLast->first == k)
				++pLast;
			// for each line
			if(!p.second.empty())
				maxV = max(maxV, p.second.back().node);
			for(const Link& l : p.second){
				if(find_if(pFirst, pLast, 
						[&](const pair<int,int>& p){ return p.second == l.node; }) == pLast){
					// non-critical edge
					modifiedSet.set(thresholdNE, k, l, p.second, maxV, gen);
				}else{
					// critical edge
					int v = modifiedSet.set(thresholdCE, k, l, p.second, maxV, gen);
					if(v==2)
						++cRmvCnt;
					else if(v==3)
						++cIncCnt;
					else if(v==4)
						++cDecCnt;
				}
			}
		}
		totalV += nv;
		totalE += ne;
		totalC += nc;
		
		// dump
		addCnt += modifiedSet.addSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.addSet, 'A', bidir);
		rmvCnt += modifiedSet.rmvSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.rmvSet, 'R', bidir);
		incCnt += modifiedSet.incSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.incSet, 'I', bidir);
		decCnt += modifiedSet.decSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.decSet, 'D', bidir);

		delete fout[i];
	}
	
	double maxCrtRatio = deltaRatio*totalE/totalC;	// the maximum allowed crtRatio by the current deltaRatio
	double minDltRatio = crtRatio*totalC/totalE;	// the minimum required deltaRatio by the current crtRatio
	if(minDltRatio > deltaRatio){
		cerr<<"  Warning: global crtRatio and deltaRatio do not match:"
			<<" max-CR="<<maxCrtRatio<<", min-DR="<<minDltRatio<<endl;
	}
	
	double te = totalE;
	double tc = totalC;
	double tm = addCnt+rmvCnt+incCnt+decCnt;
	cout << "Total vertex/edge/critical: " << totalV << "/" << totalE << "/" << totalC << "\n";
	cout<< "  delta-ratio (expected/actual/min)   ="<<deltaRatio<<"/"<<tm/te<<"/"<<minDltRatio<<"\n"
		<< "  critical-ratio (expected/actual/max)="<<crtRatio<<"/"<<(cRmvCnt+cIncCnt+cDecCnt)/tc<<"/"<<maxCrtRatio<<"\n"
		<< "  good-ratio (expected/actual)        ="<<goodRatio<<"/"<<(addCnt+decCnt)/tm<<"\n"
		<< "  ew-ratio (expected/actual)          ="<<ewRatio<<"/"<<(addCnt+rmvCnt)/tm<<"\n";
	//cout.width(4);
	cout.setf(ios::fixed);
	//cout<<"  type:  cnt\t(portion)\tce-cnt\t(CrtRatio)\n";
	cout << "  add e: " << addCnt << "\t(" << addCnt/tm << ")\n";
	cout << "  rmv e: " << rmvCnt << "\t(" << rmvCnt/tm << ")\t" << cRmvCnt << "\t(" << cRmvCnt/tc << ")\n";
	cout << "  inc w: " << incCnt << "\t(" << incCnt/tm << ")\t" << cIncCnt << "\t(" << cIncCnt/tc << ")\n";
	cout << "  dec w: " << decCnt << "\t(" << decCnt/tm << ")\t" << cDecCnt << "\t(" << cDecCnt/tc << ")\n";
	cout.flush();
	return true;
}

// ------ main ------

struct Option{
	int nPart;
	string graphFolder;
	string deltaPrefix; // output
	string cedgeFolder;
	
	double deltaRatio;
	double crtRatio;
	double goodRatio, ewRatio;

	bool dir;
	unsigned long seed;
	
	void parse(int argc, char* argv[]);
private:
	bool setWeight(string& method);
	bool checkRate1(double rate);
	bool checkRate2(double rate);
	bool checkRatios();
};

void Option::parse(int argc, char* argv[]){
	nPart = stoi(string(argv[1]));
	graphFolder = argv[2];
	cedgeFolder = argv[3];
	deltaPrefix = argv[4];

	deltaRatio = stod(string(argv[5]));
	crtRatio = stod(string(argv[6]));
	goodRatio = stod(string(argv[7]));
	ewRatio = stod(string(argv[8]));
	
	dir = true;
	if(argc > 9)
		seed = stoi(string(argv[9]))==1;
	seed = 1535345;
	if(argc > 10)
		seed = stoul(string(argv[10]));
	if(nPart<=0)
		throw invalid_argument("Given number of parts does not make sense.");
	if(!checkRatios())
		throw invalid_argument("Given rates do not make sense.");
}
bool Option::checkRate1(double rate){
	return 0.0 <= rate;
}
bool Option::checkRate2(double rate){
	return 0.0 <= rate && rate <= 1.0;
}
bool Option::checkRatios(){
	bool flag = checkRate2(deltaRatio) && checkRate2(crtRatio)
			&& checkRate2(goodRatio) && checkRate2(ewRatio);
	
	return flag;
}

int main(int argc, char* argv[]){
	if(argc < 9 || argc > 11){
		cerr << "Wrong usage.\n"
				"Usage: <#parts> <graph-folder> <ce-folder> <delta-prefix> <delta-rate> <crt-rate> <good-rate> <ew-rate> [dir] [random-seed]"
				<< endl;
		cerr << "  <#parts>: number of parts the graphs are separated (the number of files to operate).\n"
				"  <graph-folder>: the folder of graphs.\n"
				"  <crt-folder>: the folder for the critical edges (filename pattern 'cedge-<ID>').\n"
				"  <delta-prefix>: the path and name prefix of generated delta graphs, naming format: \"<delta-prefix>-<ID>\".\n"
				"  <delta-rate>: the rate of changed edges.\n"
				"  <crt-rate>: the maxmium ratio of changed critical edges (among the critical edges).\n"
				"  <good-rate>: the ratio of good changed edges\n"
				"  <ew-rate>: the ratio of edges among all changed edges (edge add/remove vs. weight increase/decrease)\n"
				"  [dir]: (=1) whether it is a directional graph\n"
				"  [random-seed]: (=1535345) seed for random numbers\n"
				"i.e.: ./deltaGen.exe 2 graph/ cedge/ delta/rd 0.05 0 0.3 0 0.7 123456 // do not touch critical edges, all bad change, 70% are A/D edges\n"
				"i.e.: ./deltaGen.exe 1 ../input/g1 ../ref/g1 ../input/g1/d2 0.01 0.2 0.3 0.3 // change 20% critical edges, 30% good change, 30% are A/D edges\n"
				<< endl;
		return 1;
	}
	Option opt;
	try{
		opt.parse(argc, argv);
	} catch(exception& e){
		cerr << e.what() << endl;
		return 2;
	}
	ios_base::sync_with_stdio(false);
	
	cout << "Loading " << opt.nPart << " parts, from folder: " << opt.graphFolder << endl;
	bool flag = changeGraph(opt.graphFolder, opt.cedgeFolder, opt.deltaPrefix,
		opt.nPart, opt.seed,
		opt.deltaRatio, opt.crtRatio, opt.goodRatio, opt.ewRatio, !opt.dir);

	if(flag)
		cout << "success " << opt.nPart  << " file(s)."<<endl;
	return flag ? 0 : 3;
}

