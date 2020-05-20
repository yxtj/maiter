add_library(worker
	worker.cc
	worker_handler.cc
	worker_checkpoint.cc
)
add_dependencies(worker msg ${CMAKE_THREAD_LIBS_INIT})