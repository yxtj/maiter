protobuf_generate_cpp(MTYPE_PB_SRC MTYPE_PB_HDR mtype.proto)
add_custom_target(mtype_proto DEPENDS ${MTYPE_PB_HDR})

protobuf_generate_cpp(COMMON_PB_SRC COMMON_PB_HDR common.proto)
add_custom_target(common_proto DEPENDS ${COMMON_PB_HDR})

protobuf_generate_cpp(MESSAGE_PB_SRC MESSAGE_PB_HDR message.proto)
add_custom_target(message_proto DEPENDS ${MESSAGE_PB_HDR})

include_directories(${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "${MAITER_SOURCE_DIR}, ${CMAKE_CURRENT_BINARY_DIR}")

add_library(msg
	${MTYPE_PB_SRC} ${MTYPE_PB_HDR}
	${COMMON_PB_SRC} ${COMMON_PB_HDR}
	${MESSAGE_PB_SRC} ${MESSAGE_PB_HDR}
)
add_dependencies(msg
	mtype_proto
	common_proto
	message_proto
)
