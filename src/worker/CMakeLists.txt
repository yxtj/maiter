add_library(worker
			worker.cc
			worker_handler.cc
			worker_checkpoint.cc
			)
add_dependencies(worker msg)
