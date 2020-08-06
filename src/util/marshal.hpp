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

// String Marshal

template<class T, class Enable = void>
struct StringMarshal : public MarshalBase {
	void marshal(const T& t, std::string* out){
		*out = std::to_string(t);
	}
	void unmarshal(const StringPiece& s, T* t){
		unmarshal(s.data, t);
	}
	void unmarshal(const std::string& s, T* t){
		// ERROR
	}
};

template<>
struct StringMarshal<int, void> : public MarshalBase {
	void marshal(const int& t, std::string* out){
		*out = std::to_string(t);
	}
	void unmarshal(const StringPiece& s, int* t){
		unmarshal(s.data, t);
	}
	void unmarshal(const std::string& s, int* t){
		*t = std::stoi(s);
	}
};
template<class T>
struct StringMarshal<T, typename std::enable_if<std::is_integral<T>::value >::type> : public MarshalBase{
	void marshal(const T& t, std::string* out){
		*out = std::to_string(t);
	}
	void unmarshal(const StringPiece& s, T* t){
		unmarshal(s.data, t);
	}
	void unmarshal(const std::string& s, T* t){
		*t = std::stoll(s);
	}
};

template<>
struct StringMarshal<float, void> : public MarshalBase{
	void marshal(const float& t, std::string* out){
		*out = std::to_string(t);
	}
	void unmarshal(const StringPiece& s, float* t){
		unmarshal(s.data, t);
	}
	void unmarshal(const std::string& s, float* t){
		*t = std::stof(s);
	}
};
template<>
struct StringMarshal<double, void> : public MarshalBase{
	void marshal(const double& t, std::string* out){
		*out = std::to_string(t);
	}
	void unmarshal(const StringPiece& s, double* t){
		unmarshal(s.data, t);
	}
	void unmarshal(const std::string& s, double* t){
		*t = std::stod(s);
	}
};


} //namespace dsm

#endif /* UTIL_MARSHAL_HPP_ */
