/*
 * IterateKernel.h
 *
 *  Created on: Dec 4, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_ITERATEKERNEL_H_
#define KERNEL_ITERATEKERNEL_H_

#include <string>
#include <vector>
#include <utility>

namespace dsm{

enum class ChangeEdgeType: char{
	ADD='A',
	REMOVE='R',
	INCREASE='I',
	DECREASE='D'
};

// NOTE: "IterateKernelBase" is not a polymorphic class (do not have virtual functions)
// So when converting to IterateKernel, static_cast<> should be used instead of dynamic_cast<>
struct IterateKernelBase {};

template <class K, class V, class D>
struct IterateKernel : public IterateKernelBase {
	virtual void read_data(std::string& line, K& k, D& data) = 0;
	// same format as the output(MaiterKernel3)
	// format: "<key>\t<value>:<delta>"
	virtual void read_init(std::string& line, K& k, V& delta, V& value) = 0;
	
	virtual void init_c(const K& k, V& delta,D& data) = 0;
	virtual const V& default_v() const = 0;
	virtual void init_v(const K& k,V& v,D& data) = 0;

	virtual void accumulate(V& a, const V& b) = 0;
	virtual bool is_selective() const;
	virtual bool better(const V& a, const V& b); // only matters when is_selective() is true
	virtual void process_delta_v(const K& k, V& dalta,V& value, D& data){}
	virtual void priority(V& pri, const V& value, const V& delta, const D& data) = 0;

	virtual V g_func(const K& k,const V& delta,const V& value, const D& data, const K& dst) = 0;
	virtual void g_func(const K& k,const V& delta,const V& value, const D& data, std::vector<std::pair<K, V> >* output);

	virtual void read_change(std::string& line, K& k, ChangeEdgeType& type, D& change){} // only D[0] make sense

	virtual K get_key(const typename D::value_type& d) = 0;
	virtual std::vector<K> get_keys(const D& d);

	virtual ~IterateKernel(){}
};

template <class K, class V, class D>
inline bool IterateKernel<K, V, D>::is_selective() const {
	return false;
}

template <class K, class V, class D>
inline bool IterateKernel<K, V, D>::better(const V& a, const V& b){
	V temp=a;
	this->accumulate(temp, b);
	return temp==a;
}

template <class K, class V, class D>
void IterateKernel<K, V, D>::g_func(const K& k,const V& delta,const V& value, const D& data,
		std::vector<std::pair<K, V> >* output)
{
	//output->clear();
	for(size_t i=0;i<data.size();++i){
		K dst = get_key(data[i]);
		output->push_back(std::make_pair(dst, g_func(k, delta, value, data, dst)));
	}
}

template <class K, class V, class D>
std::vector<K> IterateKernel<K, V, D>::get_keys(const D& d){
	std::vector<K> keys;
	keys.reserve(d.size());
	for(auto v : d){
		keys.push_back(get_key(v));
	}
	return keys;
}

} //namespace dsm

#endif /* KERNEL_ITERATEKERNEL_H_ */
