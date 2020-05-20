add_library(net
	NetworkThread.cc
	NetworkImplMPI.cc
	Task.cc
)
target_link_libraries(net ${CMAKE_THREAD_LIBS_INIT})