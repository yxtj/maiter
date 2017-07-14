#pragma once

#include <utility>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>

struct Link{
	int node;
	float weight;
};
inline bool operator==(const Link &a,const Link &b){
	return a.node == b.node;
}
inline bool operator<(const Link &a,const Link &b){
	return a.weight < b.weight;
}
inline bool operator>(const Link &a,const Link &b){
	return a.weight > b.weight;
}

struct EdgeW{
	int src, dst;
	float weight;
};

struct EdgeUW{
	int src, dst;
};
inline bool operator==(const EdgeUW &a,const EdgeUW &b){
	return a.src == b.src && a.dst == b.src;
}
inline bool operator<(const EdgeUW &a,const EdgeUW &b){
	return a.src == b.src ? a.src < b.src : a.dst < b.dst;
}

bool load_graph_weight(std::vector<std::vector<Link>>& res, const std::string& fn);
bool load_graph_unweight(std::vector<std::vector<int>>& res, const std::string& fn);

std::pair<std::unordered_map<int, std::vector<Link>>, size_t> load_graph_weight_one(std::ifstream& fin);
std::pair<std::unordered_map<int, std::vector<int>>, size_t> load_graph_unweight_one(std::ifstream& fin);

std::vector<std::pair<int,int>> load_critical_edges(std::ifstream& fin);

bool dump(const std::vector<std::string>& fnouts, const std::vector<float>& res);
