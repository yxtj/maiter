set(HEADERS
	table.h
	table_iterator.h
	table-coder.h
	table-create.h
	table-interfaces.h
	table-registry.h
	TableDescriptor.h
	TableHelper.h

	local-table.h
	deltatable.h
	statetable.h
	global-table.h
	typed-global-table.hpp
)
set(SOURCES
	TableDescriptor.cc
	local-table.cc
	global-table.cc
	table-registry.cc
)
add_library(table 
	${HEADERS} ${SOURCES})
#add_dependencies(table worker_proto)
