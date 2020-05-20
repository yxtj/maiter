#include "local-table.h"
#include "table.h"
#include "util/timer.h"
#include "util/file.h"

#ifndef NDEBUG
#include "dbg/getcallstack.h"
#endif

using namespace std;

namespace dsm {


/*
 * Local Table:
 */
//snapshot
void LocalTable::termcheck(const string& f, long* updates, double* currF2){
	VLOG(1) << "Start snapshot " << f;
//	Timer t;
	serializeToSnapshot(f, updates, currF2);
//	VLOG(1) << "Flushed snapshot " << f << " in: " << t.elapsed();

//  DLOG(INFO)<<getcallstack();
}

//Dummy stub
//void LocalTable::DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) { return; }

} //namespace dsm
