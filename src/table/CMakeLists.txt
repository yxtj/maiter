add_library(table 
			TableDescriptor.cc
            local-table.cc
            global-table.cc
            table-registry.cc)
#add_dependencies(table worker_proto)
