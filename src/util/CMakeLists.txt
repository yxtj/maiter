add_library(common
	common.cc 
	file.cc 
	stringpiece.cc
	timer.cc
	static-initializers.cc
)
target_link_libraries(common ${LZO2_LIBS})