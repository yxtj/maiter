/*
 * marshalled_map.hpp
 *
 *  Created on: Dec 5, 2015
 *      Author: tzhou
 */

#ifndef UTIL_MARSHALLED_MAP_HPP_
#define UTIL_MARSHALLED_MAP_HPP_

#include <string>
#include <unordered_map>
#include "util/common.pb.h"
#include "util/marshal.hpp"

namespace dsm{

#ifndef SWIG
class MarshalledMap{
public:
	struct MarshalledValue{
		virtual std::string ToString() const = 0;
		virtual void FromString(const std::string& s) = 0;
		virtual void set(const void* nv) = 0;
		virtual void* get() const = 0;
	};

	template<class T>
	struct MarshalledValueT: public MarshalledValue{
		MarshalledValueT() : v(new T){}
		~MarshalledValueT(){
			delete v;
		}

		std::string ToString() const{
			std::string tmp;
			m_.marshal(*v, &tmp);
			return tmp;
		}

		void FromString(const std::string& s){
			m_.unmarshal(s, v);
		}

		void* get() const{
			return v;
		}
		void set(const void *nv){
			*v = *(T*)nv;
		}

		mutable Marshal<T> m_;
		T *v;
	};

	template<class T>
	void put(const std::string& k, const T& v){
		if(serialized_.find(k) != serialized_.end()){
			serialized_.erase(serialized_.find(k));
		}

		if(p_.find(k) == p_.end()){
			p_[k] = new MarshalledValueT<T>;
		}

		p_[k]->set(&v);
	}

	template<class T>
	T& get(const std::string& k) const{
		if(serialized_.find(k) != serialized_.end()){
			p_[k] = new MarshalledValueT<T>;
			p_[k]->FromString(serialized_[k]);
			serialized_.erase(serialized_.find(k));
		}

		return *(T*)p_.find(k)->second->get();
	}

	bool contains(const std::string& key) const{
		return p_.find(key) != p_.end() || serialized_.find(key) != serialized_.end();
	}

	Args* ToMessage() const{
		Args* out = new Args;
		for(auto i = p_.begin(); i != p_.end(); ++i){
			Arg *p = out->add_param();
			p->set_key(i->first);
			p->set_value(i->second->ToString());
		}
		return out;
	}

	// We can't immediately deserialize the parameters passed in, since sadly we don't
	// know the type yet.  Instead, save the std::string values on the side, and de-serialize
	// on request.
	void FromMessage(const Args& p){
		for(int i = 0; i < p.param_size(); ++i){
			serialized_[p.param(i).key()] = p.param(i).value();
		}
	}

private:
	mutable std::unordered_map<std::string, MarshalledValue*> p_;
	mutable std::unordered_map<std::string, std::string> serialized_;
};
#endif

}

#endif /* UTIL_MARSHALLED_MAP_HPP_ */
