add_library(master
		master.cc
		master2.cc
		master_checkpoint.cc
		master_handler.cc
		worker-handle.cc
		run-descriptor.cc
		)
add_dependencies(master msg)
