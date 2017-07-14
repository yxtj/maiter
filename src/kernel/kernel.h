#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/marshalled_map.hpp"
//#include "IterateKernel.h"
//#include "DSMKernel.h"
#include <glog/logging.h>

#include <string>
#include <map>

namespace dsm {

template<class K, class V, class D>
class MaiterKernel;
class DSMKernel;

struct KernelInfo{
	KernelInfo(const std::string& name) : name_(name){}
	virtual ~KernelInfo(){}

	virtual DSMKernel* create() = 0;
	virtual void Run(DSMKernel* obj, const std::string& method_name) = 0;
	virtual bool has_method(const std::string& method_name) = 0;

	std::string name_;
};

template<class C, class K, class V, class D>
struct KernelInfoT: public KernelInfo{
	typedef void (C::*Method)();
	std::map<std::string, Method> methods_;
	MaiterKernel<K, V, D>* maiter;

	KernelInfoT(const std::string& name, MaiterKernel<K, V, D>* inmaiter) :
			KernelInfo(name){
		maiter = inmaiter;
	}

	DSMKernel* create(){
		return new C;
	}

	void Run(DSMKernel* obj, const std::string& method_id){
		C* p=dynamic_cast<C*>(obj);
		p->set_maiter(maiter);
		(p->*(methods_[method_id]))();
	}

	bool has_method(const std::string& name){
		return methods_.find(name) != methods_.end();
	}

	void register_method(const std::string& mname, Method m, MaiterKernel<K, V, D>* inmaiter){
		methods_[mname] = m;
	}
};

class KernelRegistry{
public:
	typedef std::map<std::string, KernelInfo*> Map;
	Map& kernels(){
		return m_;
	}
	KernelInfo* kernel(const std::string& name){
		return m_[name];
	}

	static KernelRegistry* Get();	//singleton
private:
	KernelRegistry(){}
	Map m_;
};

template<class C, class K, class V, class D>
struct KernelRegistrationHelper{
	KernelRegistrationHelper(const std::string& name, MaiterKernel<K, V, D>* maiter){
		KernelRegistry::Map& kreg = KernelRegistry::Get()->kernels();

		CHECK(kreg.find(name) == kreg.end());
		kreg[name]=new KernelInfoT<C, K, V, D>(name, maiter);
	}
};

template<class C, class K, class V, class D>
struct MethodRegistrationHelper{
	MethodRegistrationHelper(const std::string& klass, const std::string& mname, void (C::*m)(),
			MaiterKernel<K, V, D>* maiter){
		dynamic_cast<KernelInfoT<C,K,V,D>*>(KernelRegistry::Get()->kernel(klass))->
				register_method(mname,m,maiter);
//		((KernelInfoT<C, K, V, D>*)KernelRegistry::Get()->kernel(klass))
//				->register_method(mname, m, maiter);
	}
};

class RunnerRegistry{
public:
	typedef int (*KernelRunner)(ConfigData&);
	typedef std::map<std::string, KernelRunner> Map;

	KernelRunner runner(const std::string& name){
		return m_[name];
	}
	Map& runners(){
		return m_;
	}

	static RunnerRegistry* Get();	//singleton
private:
	RunnerRegistry(){}
	Map m_;
};

struct RunnerRegistrationHelper{
	RunnerRegistrationHelper(RunnerRegistry::KernelRunner k, const std::string& name){
//		RunnerRegistry::Get()->runners().insert(make_pair(name, k));
		RunnerRegistry::Get()->runners()[name] = k;
	}
};

#define REGISTER_KERNEL(klass)\
  static KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

#define REGISTER_RUNNER(r)\
  static RunnerRegistrationHelper r_helper_ ## r ## _(&r, #r);

}
#endif /* KERNEL_H_ */
