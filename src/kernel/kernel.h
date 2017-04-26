#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/marshalled_map.hpp"
//#include "IterateKernel.h"
#include "DSMKernel.h"
#include "table/table-registry.h"
#include "table/typed-global-table.hpp"
#include "table/tbl_widget/IterateKernel.h"

#include <fstream>
#include <string>
#include <map>
#include <thread>

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
		std::string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
		//cout<<"Unable to open file: " << patition_file<<endl;
		std::ifstream inFile(patition_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open file" << patition_file;
			// cerr << system("ifconfig -a | grep 192.168.*") << endl;
			exit(1); // terminate with error
		}

		std::string line;
		//read a line of the input file
		while(getline(inFile,line)){
			K key;
			V delta;
			D data;
			V value;
			std::vector<K> connection;
			maiter->iterkernel->read_data(line, key, data, connection); //invoke api, get the value of key field and data field
			maiter->iterkernel->init_v(key, value, data); //invoke api, get the initial v field value
			maiter->iterkernel->init_c(key, delta, data); //invoke api, get the initial delta v field value
			// DVLOG(3)<<"key: "<<key<<" delta: "<<delta<<" value: "<<value<<"   "<<data.size();
			table->add_ineighbor_from_out(key, value, connection);	//add "key" as an in-neighbor of all nodes in "ons"
			table->put(std::move(key), std::move(delta), std::move(value), std::move(data)); //initialize a row of the state table (a node)
		}
	}

	void corridnate_in_neighbors(TypedGlobalTable<K, V, V, D>* table){
		table->send_ineighbor_cache();
		table->clear_ineighbor_cache();
	}

	void init_table(TypedGlobalTable<K, V, V, D>* a){
		if(!a->initialized()){
			a->InitStateTable();        //initialize the local state table
		}
		a->resize(maiter->num_nodes);   //create local state table based on the input size

		read_file(a);               //initialize the state table fields based on the input data file
		corridnate_in_neighbors(a);
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
	std::vector<std::pair<K, V> > output;                    //the output buffer

public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}
	void run_loop(TypedGlobalTable<K, V, V, D>* tgt){
		tgt->ProcessUpdates();
		std::vector<Table*> localTs;
		for(int i = 0; i < tgt->num_shards(); ++i){
			if(tgt->is_local_shard(i))
				localTs.push_back(tgt->get_partition(i));
		}
		bool single=tgt->num_shards()==1;
		bool finish=false;
		tgt->helper()->signalToProcess();	//start the loop with initial data
		while(!finish){
			finish=true;
			for(Table* p : localTs)
				if(p->alive())
					finish=false;
			if(finish)	break;

			if(single){
				tgt->helper()->signalToProcess();
			}else{
				if(tgt->canProcess()){
					tgt->helper()->signalToProcess();
					tgt->resetProcessMarker();
				}
				if(tgt->canSend()){
					tgt->helper()->signalToSend();
					tgt->resetSendMarker();
				}
			}
			if(tgt->canTermCheck())
				tgt->helper()->signalToTermCheck();
			std::this_thread::sleep_for(std::chrono::duration<double>(0.1));
		}
//		DLOG(INFO)<<"pending writes: "<<tgt->pending_send_;
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
		double totalF1 = 0;	//the sum of delta_v, it should be smaller enough when iteration converges
		double totalF2 = 0;	//the sum of v, it should be larger enough when iteration converges
		std::string file = StringPrintf("%s/part-%d", maiter->output.c_str(), current_shard()); //the output path
		std::ofstream File(file);	//the output file containing the local state table infomation

		//get the iterator of the local state table
		typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());

		while(!it->done()){
//			bool cont = it->Next();
//			if(!cont) break;

			totalF1 += it->value1();
			totalF2 += it->value2();
			File << it->key() << "\t" << it->value1() << ":" << it->value2() << "\n";
			it->Next();
		}
		delete it;

		File.close();
		VLOG(0)<<"W"<<maiter->conf.worker_id()<<":\ttotal F1 : " << totalF1 << "\ttotal F2 : " << totalF2;
	}

	void run(){
		VLOG(0) << "dumping result";
		dump(maiter->table);
	}
};

template<class K, class V, class D>
class MaiterKernel4: public DSMKernel{ //the examine phase: dumping the in-neighbor list together with the values on them
private:
	MaiterKernel<K, V, D>* maiter;              //user-defined iteratekernel
public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}

	void dump(TypedGlobalTable<K, V, V, D>* a){
		std::string file = StringPrintf("%s/ilist-%d", maiter->output.c_str(), current_shard()); //the output path
		std::ofstream File(file);	//the output file containing the local state table infomation

		int nNode = 0, nNeigh = 0;
		//get the iterator of the local state table
		typename StateTable<K, V, V, D>::EntirePassIterator *it =
			dynamic_cast<typename StateTable<K, V, V, D>::EntirePassIterator*>(
					a->get_entirepass_iterator(current_shard()));

		while(!it->done()){
			File << it->key() << "\t";
			++nNode;
			const std::unordered_map<K, V>& ineigh = it->ineighbor();
			for(auto jt=ineigh.begin(); jt!=ineigh.end(); ++jt){
				File<< jt->first <<','<< jt->second <<' ';
				++nNeigh;
			}
			File<<"\n";
			it->Next();
		}
		delete it;

		File.close();
		VLOG(0)<<"W"<<maiter->conf.worker_id()<<":\tnNode : " << nNode << "\tnNeigh: " << nNeigh;
	}

	void run(){
		VLOG(0) << "dumping in-neighbors";
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
	std::string output;
	Sharder<K> *sharder;
	IterateKernel<K, V, D> *iterkernel;
	TermChecker<K, V> *termchecker;
	TypedGlobalTable<K, V, V, D> *table;

	MaiterKernel(){
		Reset();
	}
	MaiterKernel(ConfigData& inconf, int64_t nodes, double portion, std::string outdir,
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
		if(iterkernel != nullptr && termchecker != nullptr){
			KernelRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", this);
			MethodRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", "map",
					&MaiterKernel2<K, V, D>::map, this);
		}

		//dumping result to disk job
		KernelRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", this);
		MethodRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", "run",
				&MaiterKernel3<K, V, D>::run, this);

		KernelRegistrationHelper<MaiterKernel4<K, V, D>, K, V, D>("MaiterKernel4", this);
		MethodRegistrationHelper<MaiterKernel4<K, V, D>, K, V, D>("MaiterKernel4", "run",
				&MaiterKernel4<K, V, D>::run, this);

		return 0;
	}
};

}
#endif /* KERNEL_H_ */
