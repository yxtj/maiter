set(HEADERS
	common.h
	file.h
	hash.h
	marshal.hpp
	marshalled_map.hpp
#	noncopyable.h
	stringpiece.h
	timer.h
#	tuple.h
)
set(SOURCES
	common.cc 
	file.cc 
	stringpiece.cc
	timer.cc
	static-initializers.cc
)
add_library(common
	${HEADERS} ${SOURCES})