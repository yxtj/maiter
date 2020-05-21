add_library(net
	NetworkThread.cc
	NetworkImplMPI.cc
	Task.cc
)
target_compile_options(net PRIVATE ${MPI_CXX_COMPILE_FLAGS})
target_link_options(net PRIVATE ${MPI_CXX_LINK_FLAGS})
target_link_libraries(net ${CMAKE_THREAD_LIBS_INIT} ${MPI_CXX_LIBRARIES} ${Protobuf_LIBRARIES})