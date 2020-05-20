cmake_minimum_required (VERSION 3.0) 
#cmake_policy(SET CMP0014 OLD)

project (MAITER CXX) 

set (CMAKE_CXX_STANDARD 14)

# -------- Boost --------
#find_package(Boost REQUIRED COMPONENTS filesystem system program_options)
#include_directories(${Boost_INCLUDE_DIRS}) 
#message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

# -------- Protobuf --------
find_package(protobuf REQUIRED)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

message(STATUS "Protobuf include dir: ${Protobuf_INCLUDE_DIRS}")
message(STATUS "Protobuf complier: ${Protobuf_PROTOC_EXECUTABLE}")

# -------- MPI --------
set(MPI_EXECUTABLE_SUFFIX ".openmpi")
find_package(MPI REQUIRED)
message(STATUS "MPI: ${MPI_CXX_FOUND} ${MPIEXEC}")

include_directories(${MPI_CXX_INCLUDE_PATH})
# ${MPI_CXX_LINK_FLAGS}     Linking flags for MPI programs
# ${MPI_CXX_LIBRARIES}      All libraries to link MPI programs against
link_directories(${MPI_LIBDIRS})

message(STATUS "mpi-include-path ${MPI_CXX_INCLUDE_PATH}")
message(STATUS "mpi-compile-flags ${MPI_CXX_COMPILE_FLAGS}")
message(STATUS "mpi-link-flags ${MPI_CXX_LINK_FLAGS}")
message(STATUS "mpi-libs ${MPI_CXX_LIBRARIES}")

# -------- thread --------
find_package (Threads)
message(STATUS "Thread lib: ${CMAKE_THREAD_LIBS_INIT}")
# use ${CMAKE_THREAD_LISBS_INIT} for targets need thread support.

# -------- others --------
find_package(gflags REQUIRED)
find_package(glog REQUIRED)

# -------- main --------
set(CXX_DEFAULT "${CMAKE_CXX_FLAGS}")
set(CXX_DEFINE "-DHAVE_LIB_GFLAGS ")
#set(CXX_WARN "-Wall -Wno-sign-compare -Wno-unused-function -Wno-unused-result")


if( CMAKE_SIZEOF_VOID_P EQUAL 8 )
	set( ARCH_BITS 64 )
else()
	set( ARCH_BITS 32 )
endif()

#set(CMAKE_INCLUDE_CURRENT_DIR ON)

include_directories(
#    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MAITER_BINARY_DIR}
)

#set(CMAKE_CXX_FLAGS "${CXX_DEFAULT} -fPIC -fno-omit-frame-pointer -O2 -g0 ${CXX_DEFINE}")

#set(CXX_DEFAULT "${CMAKE_CXX_FLAGS}")
#set(CXX_DEFINE "-DELPP_THREAD_SAFE -DELPP_STL_LOGGING -DELPP_LOG_UNORDERED_MAP")
#set(CXX_WARN "-Wall -Wno-sign-compare -Wno-unused-function -Wno-unused-variable -Wno-unused-result")
set(CMAKE_CXX_FLAGS "${CXX_DEFAULT} ${CXX_WARN} ${CXX_DEFINE} ${CMAKE_THREAD_LIBS_INIT}")

add_subdirectory (src)

