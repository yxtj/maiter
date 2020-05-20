
set(CMAKE_INCLUDE_CURRENT_DIR ON)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
)

#add_subdirectory (dbg)
add_subdirectory (msg)
add_subdirectory (util)
add_subdirectory (net)
add_subdirectory (driver)
add_subdirectory (table)
add_subdirectory (kernel)
add_subdirectory (master)
add_subdirectory (worker)
add_subdirectory (examples)
