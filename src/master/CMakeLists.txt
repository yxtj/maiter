add_library(master
		master.cc
		worker-handle.cc)
add_dependencies(master msg)
