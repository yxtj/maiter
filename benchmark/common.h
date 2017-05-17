#include <vector>
#include <string>

struct Edge{
	int node;
	float weight;
};

inline bool operator==(const Edge &a,const Edge &b){
	return a.node == b.node;
}
inline bool operator<(const Edge &a,const Edge &b){
	return a.weight < b.weight;
}
inline bool operator>(const Edge &a,const Edge &b){
	return a.weight > b.weight;
}

bool load_graph_weight(std::vector<std::vector<Edge>>& res, const std::string& fn);

bool load_graph_unweight(std::vector<std::vector<Edge>>& res, const std::string& fn);

bool dump(const std::vector<std::string>& fnouts, const std::vector<float>& res);
