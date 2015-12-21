/*
 * ReplyHandler.cpp
 *
 *  Created on: Dec 20, 2015
 *      Author: tzhou
 */

#include <driver/tools/ReplyHandler.h>
#include <algorithm>
#include <thread>

using namespace std;

namespace dsm {

/*
 * Condition part:
 */

struct ConditionAny:public ReplyHandler::Condition{
	bool update(const int source){ return true; }
};
struct ConditionEachOne:public ReplyHandler::Condition{
	ConditionEachOne(const int num):state(num,false){}
	bool update(const int source){
		state[source]=true;
		if(all_of(state.begin(), state.end(), [](const bool b){return b;})){
			fill(state.begin(),state.end(),false);
			return true;
		}
		return false;
	}
private:
	vector<bool> state;
};
struct ConditionGeneral:public ReplyHandler::Condition{
	ConditionGeneral(const vector<int>& expect):
			state(expect.size()), expected(expect){}
	ConditionGeneral(vector<int>&& expect):
			state(expect.size()), expected(expect){}
	bool update(const int source){
		--state[source];
		if(all_of(state.begin(), state.end(),[](const int v){return v<=0;})){
			state=expected;
			return true;
		}
		return false;
	}
private:
	vector<int> state;
	vector<int> expected;
};

ReplyHandler::Condition* ReplyHandler::conditionFactory(const ConditionType ct,
		const int numSource, const std::vector<int>& expected)
{
	switch(ct){
	case ANY_ONE:
		return new ConditionAny();
	case EACH_ONE:
		return new ConditionEachOne(numSource);
	case GENERAL:
		return new ConditionGeneral(expected);
	}
	return new Condition();
}
ReplyHandler::Condition* ReplyHandler::conditionFactory(const ConditionType ct,
		const int numSource, std::vector<int>&& expected)
{
	switch(ct){
	case ANY_ONE:
		return new ConditionAny();
	case EACH_ONE:
		return new ConditionEachOne(numSource);
	case GENERAL:
		return new ConditionGeneral(expected);
	}
	return new Condition();
}

/*
 * ReplyHandler part:
 */

bool ReplyHandler::input(const int type, const int source){
	auto it=cont.find(type);
	if(it==cont.end() || it->second.activated )	return false;
	if(it->second.cond->update(source)){
		launch(it->second);
	}
	return true;
}

void ReplyHandler::addType(const int type, Condition* cond,
		std::function<void()> fn, const bool spwanThread)
{
	cont[type]=Item(fn, cond, spwanThread);
//	cont.insert(make_pair(type, Item(fn, cond, spwanThread)));
}
void ReplyHandler::removeType(const int type){
	auto it=cont.find(type);
	if(it!=cont.end())
		cont.erase(it);
}

void ReplyHandler::launch(Item& item){
	if(!item.spwanThread){
		item.fn();
	}else{
		thread t(item.fn);
		t.detach();
	}
}

} /* namespace dsm */
