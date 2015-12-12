#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/marshalled_map.hpp"
//#include "IterateKernel.h"
#include "DSMKernel.h"
#include "table/table.h"
#include "table/table-registry.h"
#include "table/typed-global-table.hpp"
#include "table/tbl_widget/IterateKernel.h"

DECLARE_string(graph_dir);

namespace dsm {


template<class K, class V, class D>
class MaiterKernel;

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
	KernelInfo* kernel(const string& name){
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

	KernelRunner runner(const string& name){
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

/**
 * Maiter Kernel part:
 */

//template<class K, class V, class D>
//class MaiterKernel0: public DSMKernel{
//private:
//	MaiterKernel<K, V, D>* maiter;
//public:
//	void run(){
//		VLOG(0) << "initializing table ";
//		init_table(maiter->table);
//	}
//};

template<class K, class V, class D>
class MaiterKernel1: public DSMKernel{           //the first phase: initialize the local state table
private:
	MaiterKernel<K, V, D>* maiter;                          //user-defined iteratekernel
public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}

	void read_file(TypedGlobalTable<K, V, V, D>* table){
		string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
		//cout<<"Unable to open file: " << patition_file<<endl;
		ifstream inFile;
		inFile.open(patition_file.c_str());
		if(!inFile){
			LOG(FATAL) << "Unable to open file" << patition_file;
//			cerr << system("ifconfig -a | grep 192.168.*") << endl;
			exit(1); // terminate with error
		}

		string line;
		//read a line of the input file
		while(getline(inFile,line)){
			K key;
			V delta;
			D data;
			V value;
			maiter->iterkernel->read_data(line, key, data); //invoke api, get the value of key field and data field
			maiter->iterkernel->init_v(key, value, data); //invoke api, get the initial v field value
			maiter->iterkernel->init_c(key, delta, data); //invoke api, get the initial delta v field value
			//cout<<"key: "<<key<<"delta: "<<delta<<"value: "<<value<<"   "<<data[0][0]<<"  "<<data[1][0]<<"   "<<data[2][0]<<endl;
			table->put(key, delta, value, data);      //initialize a row of the state table (a node)
		}
	}

	void init_table(TypedGlobalTable<K, V, V, D>* a){
		if(!a->initialized()){
			a->InitStateTable();        //initialize the local state table
		}
		a->resize(maiter->num_nodes);   //create local state table based on the input size

		read_file(a);               //initialize the state table fields based on the input data file
	}

	void run(){
		VLOG(0) << "initializing table ";
		init_table(maiter->table);
	}
};

template<class K, class V, class D>
class MaiterKernel2: public DSMKernel{ //the second phase: iterative processing of the local state table
private:
	MaiterKernel<K, V, D>* maiter;                  //user-defined iteratekernel
	vector<pair<K, V> >* output;                    //the output buffer

public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}
	virtual ~MaiterKernel2(){
		delete output;
	}
	void run_iter(const K& k, V &v1, V &v2, D &v3){
		//cout<<"delta:"<<v1<<endl;

		maiter->iterkernel->process_delta_v(k, v1, v2, v3);

		maiter->table->accumulateF2(k, v1);                               //perform v=v+delta_v
																		  // process delta_v before accumulate
		maiter->iterkernel->g_func(k, v1, v2, v3, output); //invoke api, perform g(delta_v) and send messages to out-neighbors
		//cout << " key " << k << endl;
		maiter->table->updateF1(k, maiter->iterkernel->default_v()); //perform delta_v=0, reset delta_v after delta_v has been spread out

		typename vector<pair<K, V> >::iterator iter;
		for(iter = output->begin(); iter != output->end(); iter++){ //send the buffered messages to remote state table
			pair<K, V> kvpair = *iter;
			//cout << "accumulating " << kvpair.first << " with " <<kvpair.second << endl;
			maiter->table->accumulateF1(kvpair.first, kvpair.second); //apply the output messages to remote state table
		}
		output->clear();                                                   //clear the output buffer

	}

	void run_loop(TypedGlobalTable<K, V, V, D>* a){
		Timer timer;                        //for experiment, time recording
		//double totalF1 = 0;                 //the sum of delta_v, it should be smaller and smaller as iterations go on
		double totalF2 = 0;       //the sum of v, it should be larger and larger as iterations go on
		long updates = 0;                   //for experiment, recording number of update operations
		output = new vector<pair<K, V> >;

		//the main loop for iterative update
		while(true){
			//set false, no inteligient stop scheme, which can check whether there are changes in statetable

			//get the iterator of the local state table
			typename TypedGlobalTable<K, V, V, D>::Iterator *it2 = a->get_typed_iterator(
					current_shard(), false);
			if(it2 == nullptr) break;

			//should not use for(;!it->done();it->Next()), that will skip some entry
			while(!it2->done()){
				bool cont = it2->Next();        //if we have more in the state table, we continue
				if(!cont) break;
				totalF2 += it2->value2();         //for experiment, recording the sum of v
				updates++;                      //for experiment, recording the number of updates

//                cout << "processing " << it2->key() << " " << it2->value1() << " " << it2->value2() << endl;
				run_iter(it2->key(), it2->value1(), it2->value2(), it2->value3());
			}
			delete it2;                         //delete the table iterator

			//for experiment
			//cout << "time " << timer.elapsed() << " worker " << current_shard() << " delta " << totalF1 <<
			// " progress " << totalF2 << " updates " << updates <<
			// " totalsent " << a->sent_bytes_ << " total " << endl;
		}
	}

	void map(){
		VLOG(0) << "start performing iterative update";
		run_loop(maiter->table);
	}
};

template<class K, class V, class D>
class MaiterKernel3: public DSMKernel{ //the third phase: dumping the result, write the in-memory table to disk
private:
	MaiterKernel<K, V, D>* maiter;              //user-defined iteratekernel
public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}

	void dump(TypedGlobalTable<K, V, V, D>* a){
		double totalF1 = 0; //the sum of delta_v, it should be smaller enough when iteration converges
		double totalF2 = 0;      //the sum of v, it should be larger enough when iteration converges
		ofstream File;                 //the output file containing the local state table infomation

		string file = StringPrintf("%s/part-%d", maiter->output.c_str(), current_shard()); //the output path
		File.open(file.c_str(), ios::out);

		//get the iterator of the local state table
		typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());

		while(!it->done()){
			bool cont = it->Next();
			if(!cont) break;

			totalF1 += it->value1();
			totalF2 += it->value2();
			File << it->key() << "\t" << it->value1() << ":" << it->value2() << "\n";
		}
		delete it;

		File.close();

		cout << "total F1 : " << totalF1 << endl;
		cout << "total F2 : " << totalF2 << endl;
	}

	void run(){
		VLOG(0) << "dumping result";
		dump(maiter->table);
	}
};

class ConfigData;

template<class K, class V, class D>
class MaiterKernel{
public:
	int64_t num_nodes;
	double schedule_portion;
	ConfigData conf;
	string output;
	Sharder<K> *sharder;
	IterateKernel<K, V, D> *iterkernel;
	TermChecker<K, V> *termchecker;
	TypedGlobalTable<K, V, V, D> *table;

	MaiterKernel(){
		Reset();
	}
	MaiterKernel(ConfigData& inconf, int64_t nodes, double portion, string outdir,
			Sharder<K>* insharder,                  //the user-defined partitioner
			IterateKernel<K, V, D>* initerkernel,   //the user-defined iterate kernel
			TermChecker<K, V>* intermchecker){     //the user-defined terminate checker
		Reset();

		conf = inconf;                  //configuration
		num_nodes = nodes;              //local state table size
		schedule_portion = portion;     //priority scheduling, scheduled portion
		output = outdir;                //output dir
		sharder = insharder;            //the user-defined partitioner
		iterkernel = initerkernel;      //the user-defined iterate kernel
		termchecker = intermchecker;    //the user-defined terminate checker
	}

	~MaiterKernel(){}

	void Reset(){
		num_nodes = 0;
		schedule_portion = 1;
		output = "result";
		sharder = nullptr;
		iterkernel = nullptr;
		termchecker = nullptr;
	}

public:
	int registerMaiter(){
		VLOG(0) << "Number of shards: " << conf.num_workers();
		table = CreateTable<K, V, V, D>(0, conf.num_workers(), schedule_portion, sharder,
				iterkernel, termchecker);

		//initialize table job
		KernelRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", this);
		MethodRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", "run",
				&MaiterKernel1<K, V, D>::run, this);

		//iterative update job
		if(iterkernel != nullptr){
			KernelRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", this);
			MethodRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", "map",
					&MaiterKernel2<K, V, D>::map, this);
		}

		//dumping result to disk job
		if(termchecker != nullptr){
			KernelRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", this);
			MethodRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", "run",
					&MaiterKernel3<K, V, D>::run, this);
		}

		return 0;
	}
};

}
#endif /* KERNEL_H_ */
