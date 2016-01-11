SET(GFLAGS_DIR "google-flags32")
SET(GLOG_DIR "google-logging32")
#SET(GFLAGS_DIR "google-flags64")
#SET(GLOG_DIR "google-logging64")

SET(GFLAGS_DIR "${GFLAGS_DIR}" PARENT_SCOPE)
SET(GLOG_DIR "${GLOG_DIR}" PARENT_SCOPE)

add_subdirectory ("${GFLAGS_DIR}")
include_directories(${GFLAGS_DIR})
add_subdirectory (${GLOG_DIR})
add_subdirectory (webgraph)

