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
#include <fstream>
#include <vector>
#include <algorithm>
#include <random>
#include <functional>
#include <unordered_set>

using namespace std;

// ---- load the graph data and generate the delta file

struct Edge{
	int u, v;
};

struct ModifyThreshold{
	double trivial;
	double add;
	double rmv;
};

struct ModifyEdges{
	vector<Edge> addSet;
	vector<Edge> rmvSet;
};

pair<int, vector<Edge>> parseFromLine(const string& line){
	int key;
	vector<Edge> data;
	size_t pos = line.find('\t');
	key = stoi(line.substr(0, pos));
	++pos;

	size_t spacepos;
	while((spacepos = line.find(' ', pos)) != line.npos){
		int node = stoi(line.substr(pos, spacepos - pos));
		Edge e{ key, node };
		data.push_back(e);
		pos = spacepos + 1;
	}
	return make_pair(key, data);
}

// normal return: (totalV, totalE, maxV)
// return by reference: resultSet = {addSet, rmvSet, incSet, decSet}
tuple<int, int, int> changeOne(ifstream& fin, int maxV, const ModifyThreshold& threshold,
		uniform_real_distribution<double>& rnd_prob, uniform_int_distribution<int>& rnd_node,
		mt19937& gen, ModifyEdges& resultSet)
{
	vector<Edge>& addSet = resultSet.addSet;
	vector<Edge>& rmvSet = resultSet.rmvSet;

	int totalV = 0;
	int totalE = 0;

	string line;
	while(getline(fin, line)){
		int addCnt = 0;
		totalV++;
		// cout << line << endl;
		int u;
		vector<Edge> hs;
		tie(u, hs) = parseFromLine(line);
		maxV = max(maxV, u);
		unordered_set<int> dests;
		for(Edge& e : hs){
			dests.insert(e.v);
			maxV = max(maxV, e.v);
			double r = rnd_prob(gen);
			if(r < threshold.trivial){
				continue;
			}else if(r < threshold.add){
				++addCnt;
			}else if(r < threshold.rmv){
				rmvSet.push_back(e);
			}
		}
		totalE += hs.size();
		dests.insert(u);
		//cout << hs.size() << endl;
		// add
		while(addCnt--){
			int rpt = 0;
			int newV;
			do{
				newV = rnd_node(gen) % maxV;
			}while(dests.find(newV) != dests.end() && rpt++ < 10);
			if(rpt < 10){
				addSet.push_back(Edge{ u, newV });
			}else{
				// ++failAdd;
			}
		}
	} // line
	return make_tuple(totalV, totalE, maxV);
}

void dumpChangeOneSet(ofstream& fout, const vector<Edge>& edgeSet, char type, bool bidir){
	if(!bidir){
		for(const Edge& e : edgeSet){
			fout << type << " " << e.u << "," << e.v << "\n";
		}
	}else{
		for(const Edge& e : edgeSet){
			fout << type << " " << e.u << "," << e.v << "\n";
			fout << type << " " << e.v << "," << e.u << "\n";
		}
	}
}

int changeGraph(const string& dir, const string& deltaPrefix,
		const int nPart, const int seed, const double rate,
		const double addRate, const double rmvRate, const bool bidir)
{
	vector<ifstream*> fin;
	vector<ofstream*> fout;
	for(int i = 0; i < nPart; ++i){
		fin.push_back(new ifstream(dir + "/part" + to_string(i)));
		fout.push_back(new ofstream(deltaPrefix + "-" + to_string(i)));
		if(!fin.back()->is_open()){
			cerr << "failed in opening input file: " << dir + "/part" + to_string(i) << endl;
			return 0;
		}
		if(!fout.back()->is_open()){
			cerr << "failed in opening output file: " << deltaPrefix + "-" + to_string(i) << endl;
			return 0;
		}
	}

	mt19937 gen(seed);
	uniform_real_distribution<double> rnd_prob(0.0, 1.0);
	uniform_int_distribution<int> rnd_node; // 0 to numeric_limits<int>::max()

	//double modProb=rate*(1-addRate);
	double addProb = rate * addRate, rmvProb = rate * rmvRate;

	ModifyThreshold threshold; //{ addTh, rmvTh, incTh, decTh };
	threshold.trivial = (1 - rate);
	threshold.add = threshold.trivial + addProb;
	threshold.rmv = threshold.add + rmvProb;

	int totalV = 0, totalE = 0;
	// int failAdd = 0;
	int addCnt = 0, rmvCnt = 0;

	int maxV = 0;
	string line;
	for(int i = 0; i < nPart; i++){
		// generate
		ModifyEdges modifiedSet;
		tuple<int, int, int> ret = changeOne(
				*fin[i], maxV, threshold, rnd_prob, rnd_node, gen, modifiedSet);
		totalV += get<0>(ret);
		totalE += get<1>(ret);
		maxV = max(maxV, get<2>(ret));
		delete fin[i];

		// dump
		addCnt += modifiedSet.addSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.addSet, 'A', bidir);
		rmvCnt += modifiedSet.rmvSet.size();
		dumpChangeOneSet(*fout[i], modifiedSet.rmvSet, 'R', bidir);

		delete fout[i];
	} // file

	double te = totalE;
	cout << "Total vertex/edge: " << totalV << "/" << totalE << "\n";
	cout << "  add e: " << addCnt << "\t: " << addCnt / te << "\n";
	cout << "  rmv e: " << rmvCnt << "\t: " << rmvCnt / te << endl;
	return nPart;
}

// ------ main ------

struct Option{
	string graphFolder;
	int nPart;
	string deltaPrefix;
	
	double alpha; // for power-law distribution
	
	string weight;
	double wmin, wmax;
	double rate;	// rate of changed edges
	double addRate, rmvRate;

	bool dir;
	unsigned long seed;
	
	void parse(int argc, char* argv[]);
private:
	bool setWeight(string& method);
	bool checkRate1(double rate);
	bool checkRate2(double rate);
	bool normalizeRates();
};

void Option::parse(int argc, char* argv[]){
	nPart = stoi(string(argv[1]));
	graphFolder = argv[2];
//	nNode=stoi(string(argv[2]));
	deltaPrefix = argv[3];
	rate = stod(string(argv[4]));
	addRate = stod(string(argv[5]));
	rmvRate = stod(string(argv[6]));
	dir = true;
	if(argc > 7)
		dir = stoi(string(argv[7]))==1;
	seed = 1535345;
	if(argc > 8)
		seed = stoul(string(argv[8]));
	if(!normalizeRates())
		throw invalid_argument("Given rates do not make sense.");
}
bool Option::checkRate1(double rate){
	return 0.0 <= rate;
}
bool Option::checkRate2(double rate){
	return 0.0 <= rate && rate <= 1.0;
}
bool Option::normalizeRates(){
	bool flag = checkRate2(rate)
			&& checkRate1(addRate) && checkRate1(rmvRate);
	if(!flag)
		return false;
	double total = addRate + rmvRate;
	if(total != 1.0){
		cout << "normalizing modifying rates" << endl;
		addRate /= total;
		rmvRate /= total;
	}
	return true;
}

int main(int argc, char* argv[]){
	if(argc < 7 || argc > 9){
		cerr << "Wrong usage.\n"
				"Usage: <graph-folder> <#parts> <delta-prefix> <deltaRate> <addRate> <rmvRate> [dir] [random-seed]"
				<< endl;
		cerr <<	"  <#parts>: number of parts the graphs are separated (the number of files to operate).\n"
				"  <graph-folder>: the folder of graphs.\n"
				"  <delta-prefix>: the path and name prefix of generated delta graphs, naming format: \"<delta-prefix>-<part>\".\n"
				"  <deltaRate>: the rate of changed edges.\n"
				"  <addRate>, <rmvRate>: "
				"among the changed edges the rates for edge-addition and edge-removal. "
				"They are automatically normalized.\n"
				"  [dir]: (=1) whether it is a directional graph\n"
				"  [random-seed]: (=1535345) seed for random numbers\n"
				"i.e.: ./delta-gen-uw.exe 1 graphDir delta-rd 0.05 0 1 1 123456\n"
				"i.e.: ./delta-gen-uw.exe 2 input ../delta/d2 0.01 0.2 0.8 0\n"
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

	int n = changeGraph(opt.graphFolder, opt.deltaPrefix, opt.nPart, opt.seed, opt.rate,
			opt.addRate, opt.rmvRate, !opt.dir);

	cout << "success " << n << " files. fail " << opt.nPart - n << " files." << endl;
	return n > 0 ? 0 : 3;
}
