#include "common.h"
#include <fstream>
#include <algorithm>
#include <iostream>

using namespace std;

bool load_graph_weight(std::vector<std::vector<Edge>>& res, const std::string& fn){
	ifstream fin(fn);
	if(!fin){
		return false;
	}
	string line;
	while(getline(fin, line)){
		if(line.size()<2)
			continue;
		size_t pos = line.find('\t');
		int k = stoi(line.substr(0, pos));
		++pos;
		vector<Edge> temp;
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			size_t cut = line.find(',', pos + 1);
			int node=stoi(line.substr(pos, cut - pos));
			float weight=stof(line.substr(cut + 1, spacepos - cut - 1));
			Edge e{node, weight};
			temp.push_back(e);
			pos = spacepos + 1;
		}
		if(res.size() <= k)	// k starts from 0
			res.resize(k+1);	// fill the empty holes
		res[k]=move(temp);
	}
	return true;
}

bool load_graph_unweight(std::vector<std::vector<int>>& res, const std::string& fn){
	ifstream fin(fn);
	if(!fin){
		return false;
	}
	string line;
	while(getline(fin, line)){
		if(line.size()<2)
			continue;
		size_t pos = line.find('\t');
		int k = stoi(line.substr(0, pos));
		++pos;
		vector<int> temp;
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			int node=stoi(line.substr(pos, spacepos - pos));
			temp.push_back(node);
			pos = spacepos + 1;
		}
		if(res.size() < k)	// k starts from 0
			res.resize(k);	// fill the empty holes
		res.push_back(move(temp));
	}
	return true;
}

bool dump(const std::vector<std::string>& fnouts, const std::vector<float>& res){
	size_t parts=fnouts.size();
	vector<ofstream*> fouts;
	for(size_t i=0;i<parts;++i){
		ofstream* pf=new ofstream(fnouts[i]);
		if(!pf || !pf->is_open())
			return false;
		fouts.push_back(pf);
	}
	size_t size=res.size();
	for(size_t i=0;i<size;++i){
		(*fouts[i%parts])<<i<<"\t0:"<<res[i]<<"\n";
	}
	for(size_t i=0;i<parts;++i)
		delete fouts[i];
	return true;
}
