/*
 * Dispatcher.h
 *
 *  Created on: Dec 15, 2015
 *      Author: tzhou
 */

#ifndef DRIVER_DISPATCHER_HPP_
#define DRIVER_DISPATCHER_HPP_

#include <unordered_map>
#include <functional>
#include <thread>
#include <functional>

static constexpr int InitTypeNum=50;

namespace dsm {

template<class... Params>
class Dispatcher{
public:
	typedef std::function<void(Params...)> callback_t;

	Dispatcher(float loadFactor=0.8f):callbacks_(InitTypeNum){
		callbacks_.max_load_factor(loadFactor);
	}

	void registerDispFun(const int type, callback_t cb, const bool spawnThread=false);
	void unregisterDispFun(const int type);
	void clear();

	void changeThreadPolicy(const int type, const bool spawnThread);
	bool canHandle(const int type) const;

	bool receiveData(const int type, Params... param) const;
	void runWithData(const int type, Params... param) const;
private:
	std::unordered_map<int, std::pair<callback_t, bool> > callbacks_;

	inline void launch(const std::pair<callback_t, bool>& target, Params... param) const;
};

template<class... Params>
void Dispatcher<Params...>::registerDispFun(const int type, callback_t cb, const bool spawnThread){
	callbacks_[type]=make_pair(cb,spawnThread);
}

template<class... Params>
void Dispatcher<Params...>::unregisterDispFun(const int type){
	callbacks_.erase(type);
}

template<class... Params>
void Dispatcher<Params...>::clear(){
	callbacks_.clear();
}

template<class... Params>
void Dispatcher<Params...>::changeThreadPolicy(const int type, const bool spawnThread){
//	auto it=callbacks_.find(type);
//	if(it!=)
	callbacks_[type].second=spawnThread;
}

template<class... Params>
bool Dispatcher<Params...>::canHandle(const int type) const{
	return callbacks_.find(type)!=callbacks_.end();
}

template<class... Params>
bool Dispatcher<Params...>::receiveData(int type, Params... param) const{
	auto it=callbacks_.find(type);
	if(it!=callbacks_.end()){
		launch(it->second, param...);
		return true;
	}
	return false;
}

template<class... Params>
void Dispatcher<Params...>::runWithData(int type, Params... param) const{
	launch(callbacks_.at(type),param...);
}

template<class... Params>
void Dispatcher<Params...>::launch(
		const std::pair<callback_t, bool>& target, Params... param) const{
	if(!target.second){
		//do not need a new thread (normal case, put above for performance)
		target.first(param...);
	}else{
//		std::thread t(target.first, param...);
		// Use std::bind to pass any type of param correctly to std::thread.
		// std::thread's construct require explicit std::ref(x) on &x. But
		// std::ref(x) invoke compile failure when x is a pointer.
		std::function<void()> f=std::bind(target.first, param...);
		std::thread t(f);
		t.detach();
	}
}

} /* namespace dsm */

#endif /* DRIVER_DISPATCHER_HPP_ */
