#include "common.h"
#include <fstream>
#include <algorithm>
#include <iostream>

using namespace std;

bool load_graph_weight(std::vector<std::vector<Link>>& res, const std::string& fn){
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
		vector<Link> temp;
		size_t spacepos;
		while((spacepos = line.find(' ', pos)) != line.npos){
			size_t cut = line.find(',', pos + 1);
			int node=stoi(line.substr(pos, cut - pos));
			float weight=stof(line.substr(cut + 1, spacepos - cut - 1));
			Link e{node, weight};
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

std::pair<std::unordered_map<int, std::vector<Link>>, size_t> load_graph_weight_one(std::ifstream& fin){
	unordered_map<int, vector<Link>> res;
	size_t n = 0;
	string line;
	while(getline(fin, line)){
		if(line.size()<2)
			continue;
		size_t pos = line.find('\t');
		int k = stoi(line.substr(0, pos));
		++pos;
		size_t spacepos;
		vector<Link> temp;
		while((spacepos = line.find(' ', pos)) != line.npos){
			size_t cut = line.find(',', pos + 1);
			int node=stoi(line.substr(pos, cut - pos));
			float weight=stof(line.substr(cut + 1, spacepos - cut - 1));
			temp.push_back(Link{node, weight});
			pos = spacepos + 1;
		}
		n += temp.size();
		sort(temp.begin(), temp.end(), [](const Link& a, const Link& b){
			return a.node < b.node;
		});
		res[k] = temp;
	}
	return make_pair(res, n);
}

std::pair<std::unordered_map<int, std::vector<int>>, size_t> load_graph_unweight_one(std::ifstream& fin){
	unordered_map<int, vector<int>> res;
	size_t n = 0;
	string line;
	while(getline(fin, line)){
		if(line.size()<2)
			continue;
		size_t pos = line.find('\t');
		int k = stoi(line.substr(0, pos));
		++pos;
		size_t spacepos;
		vector<int> temp;
		while((spacepos = line.find(' ', pos)) != line.npos){
			int node=stoi(line.substr(pos, spacepos - pos));
			temp.push_back(node);
			pos = spacepos + 1;
		}
		n += temp.size();
		sort(temp.begin(), temp.end());
		res[k] = temp;
	}
	return make_pair(res, n);
}

std::vector<std::pair<int,int>> load_critical_edges(std::ifstream& fin){
	std::vector<std::pair<int,int>> res;
	string line;
	while(getline(fin, line)){
		if(line.size() < 2)
			continue;
		size_t p=line.find(' ');
		int s=stoi(line.substr(0, p));
		int d=stoi(line.substr(p + 1));
		res.emplace_back(s, d);
	}
	sort(res.begin(), res.end());
	return res;
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
