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

// NOTE: "IterateKernelBase" is not a polymorphic class (do not have virtual functions)
// So when converting to IterateKernel, static_cast<> should be used instead of dynamic_cast<>
struct IterateKernelBase {};

#ifndef SWIG

template <class K, class V, class D>
struct IterateKernel : public IterateKernelBase {
  virtual void read_data(std::string& line, K& k, D& data) = 0;
  virtual void init_c(const K& k, V& delta,D& data) = 0;
  virtual const V& default_v() const = 0;
  virtual void init_v(const K& k,V& v,D& data) = 0;
  virtual void accumulate(V& a, const V& b) = 0;
  virtual void process_delta_v(const K& k, V& dalta,V& value, D& data){}
  virtual void priority(V& pri, const V& value, const V& delta) = 0;
  virtual void g_func(const K& k,const V& delta,const V& value, const D& data, std::vector<std::pair<K, V> >* output) = 0;
  virtual ~IterateKernel(){}
};

#endif		//#ifdef SWIG / #else

} //namespace dsm

#endif /* KERNEL_ITERATEKERNEL_H_ */
