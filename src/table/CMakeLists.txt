add_library(table 
            local-table.cc
            global-table.cc
            table-registry.cc)
add_dependencies(table worker_proto)
