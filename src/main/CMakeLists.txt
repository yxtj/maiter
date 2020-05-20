add_executable(maiter main.cc)

# we need to resolve static initializers, so glob all the symbols together with -whole-archive
target_link_libraries(maiter
		-Wl,-whole-archive dbg common net msg driver table kernel worker master example
		-Wl,-no-whole-archive)
target_link_libraries(maiter gflags glog)
