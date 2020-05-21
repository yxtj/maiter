add_library(example
	pagerank.cc
	shortestpath.cc
	#adsorption.cc
	#katz.cc
	#simrank.cc
	#con_component.cc
)

add_dependencies(example common_proto)

