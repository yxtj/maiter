add_library(master
	master.h
	run-descriptor.h
	worker-handle.h
		master.cc
		master2.cc
		master_checkpoint.cc
		master_handler.cc
		worker-handle.cc
		run-descriptor.cc
		)
add_dependencies(master msg)
