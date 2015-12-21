add_library(master
		master.cc
		master2.cc
		master_checkpoint.cc
		worker-handle.cc
		)
add_dependencies(master msg)
