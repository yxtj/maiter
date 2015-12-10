add_library(master
		master.cc
		worker-handle.cc
		${WORKER_PB_SRC} ${WORKER_PB_HDR})
add_dependencies(master worker_proto)
