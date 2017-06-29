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
	float w;
};

struct ModifyThreshold{
	double trivial;
	double add;
	double rmv;
	double inc;
	double dec;
};

struct ModifyEdges{
	vector<Edge> addSet;
	vector<Edge> rmvSet;
	vector<Edge> incSet;
	vector<Edge> decSet;
};

pair<int, vector<Edge>> parseFromLine(const string& line){
	int key;
	vector<Edge> data;
	size_t pos = line.find('\t');
	key = stoi(line.substr(0, pos));
	++pos;

	size_t spacepos;
	while((spacepos = line.find(' ', pos)) != line.npos){
		size_t cut = line.find(',', pos + 1);
		int node = stoi(line.substr(pos, cut - pos));
		float weight = stof(line.substr(cut + 1, spacepos - cut - 1));
		Edge e{ key, node, weight };
		data.push_back(e);
		pos = spacepos + 1;
	}
	return make_pair(key, data);
}

// normal return: (totalV, totalE, maxV)
// return by reference: resultSet = {addSet, rmvSet, incSet, decSet}
tuple<int, int, int> changeOne(ifstream& fin, int maxV, const ModifyThreshold& threshold,
		uniform_real_distribution<double>& rnd_prob, uniform_int_distribution<int>& rnd_node,
		uniform_real_distribution<float>& rnd_weight, mt19937& gen,
		ModifyEdges& resultSet)
{
	vector<Edge>& addSet = resultSet.addSet;
	vector<Edge>& rmvSet = resultSet.rmvSet;
	vector<Edge>& incSet = resultSet.incSet;
	vector<Edge>& decSet = resultSet.decSet;

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
			}else if(r < threshold.inc){
				e.w = e.w * (1 + rnd_weight(gen));
				incSet.push_back(e);
			}else if(r < threshold.dec){
				e.w = e.w * rnd_weight(gen);
				decSet.push_back(e);
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
				Edge e{ u, newV, rnd_weight(gen) };
				addSet.push_back(e);
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
			fout << type << " " << e.u << "," << e.v << "," << e.w << "\n";
		}
	}else{
		for(const Edge& e : edgeSet){
			fout << type << " " << e.u << "," << e.v << "," << e.w << "\n";
			fout << type << " " << e.v << "," << e.u << "," << e.w << "\n";
		}
	}
}

int changeGraph(const string& dir, const string& deltaPrefix,
		const int nPart, const int seed, const double rate,
		const double addRate, const double rmvRate, const double incRate, const double decRate, const bool bidir)
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
	uniform_real_distribution<float> rnd_weight(0, 1);

	//double modProb=rate*(1-addRate);
	double addProb = rate * addRate, rmvProb = rate * rmvRate;
	double incProb = rate * incRate, decProb = rate * decRate;

	ModifyThreshold threshold; //{ addTh, rmvTh, incTh, decTh };
	threshold.trivial = (1 - rate);
	threshold.add = threshold.trivial + addProb;
	threshold.rmv = threshold.add + rmvProb;
	threshold.inc = threshold.rmv + incProb;
	threshold.dec = threshold.inc + decProb;

	int totalV = 0, totalE = 0;
	// int failAdd = 0;
	int addCnt = 0, rmvCnt = 0, incCnt = 0, decCnt = 0;

	int maxV = 0;
	string line;
	for(int i = 0; i < nPart; i++){
		// generate
		ModifyEdges modifiedSet;
		tuple<int, int, int> ret = changeOne(
				*fin[i], maxV, threshold, rnd_prob, rnd_node, rnd_weight, gen, modifiedSet);
		totalV += get<0>(ret);
		totalE += get<1>(ret);
		maxV = max(maxV, get<2>(ret));
		delete fin[i];

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
	} // file

	double te = totalE;
	cout << "Total vertex/edge: " << totalV << "/" << totalE << "\n";
	cout << "  add e: " << addCnt << "\t: " << addCnt / te << "\n";
	cout << "  rmv e: " << rmvCnt << "\t: " << rmvCnt / te << "\n";
	cout << "  inc w: " << incCnt << "\t: " << incCnt / te << "\n";
	cout << "  dec w: " << decCnt << "\t: " << decCnt / te << endl;
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
	double addRate, rmvRate, incRate, decRate;

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
	incRate = stod(string(argv[7]));
	decRate = stod(string(argv[8]));
	dir = true;
	if(argc > 9)
		dir = stoi(string(argv[9]))==1;
	seed = 1535345;
	if(argc > 10)
		seed = stoul(string(argv[10]));
	if(!normalizeRates())
		throw invalid_argument("Given rates do not make sense.");
}
bool Option::setWeight(string& method){
	if(method == "no"){
		weight = "no";
	}else if(method.substr(0, 7) == "weight:"){
		weight = "weight";
		size_t p = method.find(',', 7);
		wmin = stod(method.substr(7, p - 7));
		wmax = stod(method.substr(p + 1));
	}else{
		return false;
	}
	return true;
}
bool Option::checkRate1(double rate){
	return 0.0 <= rate;
}
bool Option::checkRate2(double rate){
	return 0.0 <= rate && rate <= 1.0;
}
bool Option::normalizeRates(){
	bool flag = checkRate2(rate)
			&& checkRate1(addRate) && checkRate1(rmvRate)
			&& checkRate1(incRate) && checkRate1(decRate);
	if(!flag)
		return false;
	double total = addRate + rmvRate + incRate + decRate;
	if(total != 1.0){
		cout << "normalizing modifying rates" << endl;
		addRate /= total;
		rmvRate /= total;
		incRate /= total;
		decRate /= total;
	}
	return true;
}

int main(int argc, char* argv[]){
	if(argc < 9 || argc > 11){
		cerr << "Wrong usage.\n"
				"Usage: <graph-folder> <#parts> <delta-prefix> <deltaRate> <addRate> <rmvRate> <incRate> <decRate> [dir] [random-seed]"
				<< endl;
		cerr <<	"  <#parts>: number of parts the graphs are separated (the number of files to operate).\n"
				"  <graph-folder>: the folder of graphs.\n"
				"  <delta-prefix>: the path and name prefix of generated delta graphs, naming format: \"<delta-prefix>-<part>\".\n"
				"  <deltaRate>: the rate of changed edges.\n"
				"  <addRate>, <rmvRate>, <incRate>, <decRate>: "
				"among the changed edges the rates for edge-addition, edge-removal, weight-increase and weight-decrease. "
				"They are automatically normalized.\n"
				"  [dir]: (=1) whether it is a directional graph\n"
				"  [random-seed]: (=1535345) seed for random numbers\n"
				"i.e.: ./delta-gen.exe 1 graphDir delta-rd 0.05 0 0.3 0 0.7 123456\n"
				"i.e.: ./delta-gen.exe 2 input ../delta/d2 0.01 0.2 0.2 0.3 0.3\n"
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
			opt.addRate, opt.rmvRate, opt.incRate, opt.decRate, !opt.dir);

	cout << "success " << n << " files. fail " << opt.nPart - n << " files." << endl;
	return n > 0 ? 0 : 3;
}

