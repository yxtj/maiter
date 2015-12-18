add_library(worker
			worker.cc
			worker_handler.cc
			)
add_dependencies(worker msg)
