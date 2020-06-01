#include "client/client.h"

void RegisterExamples();

using namespace dsm;

int Adsorption(ConfigData& conf);
//int ConnectedComponen(ConfigData& conf);
int Katz(ConfigData& conf);
int Pagerank(ConfigData& conf);
int Shortestpath(ConfigData& conf);
int Simrank(ConfigData& conf);

/*
#include "examples/examples.pb.h"

using namespace dsm;

DECLARE_int32(shards);
DECLARE_int32(iterations);
DECLARE_int32(block_size);
DECLARE_int32(edge_size);
DECLARE_bool(build_graph);

namespace dsm {
template <>
struct Marshal<Bucket> : public MarshalBase {
  static void marshal(const Bucket& t, string *out) { t.SerializePartialToString(out); }
  static void unmarshal(const StringPiece& s, Bucket* t) { t->ParseFromArray(s.data, s.len); }
};
}
*/