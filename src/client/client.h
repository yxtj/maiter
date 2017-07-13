#pragma once
#ifndef CLIENT_H_
#define CLIENT_H_

#include "table/table-registry.h"
#include "util/common.h"
#include "util/file.h"

#include "worker/worker.h"
#include "master/master.h"
#include "master/master_tf.hpp"

#include "kernel/maiter-kernel.h"
#include "table/tbl_widget/sharder_impl.hpp"
#include "table/tbl_widget/term_checker_impl.hpp"

//#include <iostream>
//#include <fstream>

#include "dbg/getcallstack.h"

#ifndef SWIG
DECLARE_int32(shards);
DECLARE_int32(iterations);
#endif

struct Link{
	Link(int inend, float inweight) :
			end(inend), weight(inweight){
	}
	int end;
	float weight;
};
inline bool operator==(const Link& lth, const Link& rth){
	return lth.end == rth.end;
}
inline bool operator==(const Link& lth, const int rth){
	return lth.end == rth;
}

// These are expanded by the preprocessor; these macro definitions
// are just for documentation.

// Run the given block of code on a single shard of 'table'.
#define PRunOne(table, code)

// Run the given block of code once for all shards of 'table'.
#define PRunAll(table, code)

// The (value : table) entries in bindings are evaluated once for
// each entry in table.  'code' is a code block that is
// executed with the bindings provided, once for each table entry.
#define PMap(bindings, code)


#endif /* CLIENT_H_ */
