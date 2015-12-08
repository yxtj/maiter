/*
 * trigger.h
 *
 *  Created on: Dec 4, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_TRIGGER_H_
#define KERNEL_TRIGGER_H_

namespace dsm {

class Table;
class TableHelper;

typedef int TriggerID;

struct TriggerBase{
	Table *table;
	TableHelper *helper;
	TriggerID triggerid;

	TriggerBase() : table(nullptr), helper(nullptr), triggerid(-1), enabled_(true){
	}
	virtual void enable(bool enabled__){
		enabled_ = enabled__;
	}
	virtual bool enabled(){
		return enabled_;
	}
	virtual ~TriggerBase(){
	}
private:
	bool enabled_;
};

// Triggers are registered at table initialization time, and
// are executed in response to changes to a table.s
//
// When firing, triggers are activated in the order specified at
// initialization time.
template<class K, class V>
struct Trigger: public TriggerBase{
	virtual bool Fire(const K& k, const V& current, V& update) = 0;
};

//#ifdef SWIGPYTHON
//template <class K, class V> class TriggerDescriptor : public Trigger;
//#endif

} //namespace dsm

#endif /* KERNEL_TRIGGER_H_ */
