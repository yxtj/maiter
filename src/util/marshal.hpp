/*
 * marshal.hpp
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef UTIL_MARSHAL_HPP_
#define UTIL_MARSHAL_HPP_

#include "util/stringpiece.h"
#include <google/protobuf/message.h>
#include <string>
#include <type_traits>

namespace dsm {

struct MarshalBase{};

template<class T, class Enable = void>
struct Marshal: public MarshalBase{
	virtual void marshal(const T& t, std::string* out){
		//GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
		out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
	}

	virtual void unmarshal(const StringPiece& s, T *t){
		//GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
		*t = *reinterpret_cast<const T*>(s.data.c_str());
	}
	virtual ~Marshal(){}
};

template<class T>
struct Marshal<T, typename std::enable_if<std::is_base_of<std::string, T>::value >::type> : public MarshalBase{
	void marshal(const std::string& t, std::string *out){
		*out = t;
	}
	void unmarshal(const StringPiece& s, std::string *t){
		t->assign(s.data.c_str(), s.len);
	}
};

template<class T>
struct Marshal<T, typename std::enable_if<std::is_base_of<google::protobuf::Message, T>::value >::type> : public MarshalBase{
	void marshal(const google::protobuf::Message& t, std::string *out){
		t.SerializePartialToString(out);
	}
	void unmarshal(const StringPiece& s, google::protobuf::Message* t){
		t->ParseFromArray(s.data.c_str(), s.len);
	}
};

template<class T>
std::string marshal(Marshal<T>* m, const T& t){
	std::string out;
	m->marshal(t, &out);
	return out;
}

template<class T>
T unmarshal(Marshal<T>* m, const StringPiece& s){
	T out;
	m->unmarshal(s, &out);
	return out;
}

} //namespace dsm

#endif /* UTIL_MARSHAL_HPP_ */
