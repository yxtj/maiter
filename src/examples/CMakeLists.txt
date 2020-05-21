#set(EXAMPLE_LIBS 
#	${EXAMPLE_LIBS} 
#	${MPI_LIBS}
#	${BLAS_LIBS}
#  	${PROTOBUF_LIBS})  	  	
#set(EXAMPLE_LIBS ${EXAMPLE_LIBS} util)

#add_custom_target(example_proto DEPENDS ${EXAMPLE_PB_HDR})

add_library(example 
	pagerank.cc
	shortestpath.cc
	#adsorption.cc
	#katz.cc
	#simrank.cc
	#con_component.cc
)
            
add_dependencies(example common_proto)

