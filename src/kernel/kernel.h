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
DECLARE_string(init_dir);
DECLARE_string(delta_name);

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
		std::ifstream inFile(patition_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open file" << patition_file;
		}

		std::string line;
		//read a line of the input file
		while(getline(inFile,line)){
			if(line.empty())
				continue;
			K key;
			V delta;
			D data;
			V value;
			maiter->iterkernel->read_data(line, key, data); //invoke api, get the value of key field and data field
			maiter->iterkernel->init_v(key, value, data); //invoke api, get the initial v field value
			maiter->iterkernel->init_c(key, delta, data); //invoke api, get the initial delta v field value
			std::vector<K> iconnection=maiter->iterkernel->get_keys(data);
			// DVLOG(3)<<"key: "<<key<<" delta: "<<delta<<" value: "<<value<<"   "<<data.size();
			//table->add_ineighbor_from_out(key, value, iconnection);	//add "key" as an in-neighbor of all nodes in "ons"
			table->put(std::move(key), std::move(delta), std::move(value), std::move(data)); //initialize a row of the state table (a node)
		}
	}

	void load_initial(TypedGlobalTable<K, V, V, D>* table,
			const bool use_initial_delta, const bool use_v_for_delta)
	{
		std::string init_file = StringPrintf("%s/part-%d", FLAGS_init_dir.c_str(), current_shard());
		std::ifstream inFile(init_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open file" << init_file;
		}

		std::string line;
		//read a line of the input file
		while(getline(inFile,line)){
			if(line.empty())
				continue;
			K key;
			V delta;
			V value;
			// same format as the output(MaiterKernel3)
			// format: "<key>\t<value>:<delta>"
			maiter->iterkernel->read_init(line, key, delta, value);
			if(use_initial_delta){
				D d = table->getF3(key);
				maiter->iterkernel->init_c(key, delta, d);
			}else if(use_v_for_delta){
				// For min/max case, delta should be set to value
				delta = value;
			}
			table->updateF1(key, delta);
			table->updateF2(key, value);
		}
	}

	void corridnate_in_neighbors(TypedGlobalTable<K, V, V, D>* table, const bool non_default_in_neighbor){
		// whether to send processed (g_func) delta to out-neighbors
		table->fill_ineighbor_cache(non_default_in_neighbor);
		table->allpy_inneighbor_cache_local();
		table->send_ineighbor_cache_remote();
		table->clear_ineighbor_cache();
	}

	void init_table(TypedGlobalTable<K, V, V, D>* a){
		if(!a->initialized()){
			a->InitStateTable();        //initialize the local state table
		}
		a->resize(maiter->num_nodes);   //create local state table based on the input size

		VLOG(0)<<"loading graphs on "<<current_shard();
		read_file(a);               //initialize the state table fields based on the input data file
		bool load_initial_value=!FLAGS_init_dir.empty();
		bool is_minmax_accumulate = maiter->iterkernel->is_minmax_accumulate();
		if(load_initial_value){
			VLOG(0)<<"loading initial values on "<<current_shard();
			load_initial(a, false, is_minmax_accumulate);
		}
		// if delta is loaded from an initialization file, targets should remember processed input values
		corridnate_in_neighbors(a, load_initial_value && is_minmax_accumulate);
	}

	void run(){
		VLOG(0) << "initializing table on "<<current_shard();
		init_table(maiter->table);
	}
};

// load and apply the delta graph
template<class K, class V, class D>
class MaiterKernelLoadDeltaGraph: public DSMKernel{
private:
	MaiterKernel<K, V, D>* maiter;                          //user-defined iteratekernel
public:
	void set_maiter(MaiterKernel<K, V, D>* inmaiter){
		maiter = inmaiter;
	}

	std::vector<std::tuple<K, ChangeEdgeType, D>> read_delta(TypedGlobalTable<K, V, V, D>* table){
		std::string patition_file = StringPrintf("%s/%s-%d",
			FLAGS_graph_dir.c_str(), FLAGS_delta_name.c_str(), current_shard());
		std::ifstream inFile(patition_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open file" << patition_file;
		}

		VLOG(1)<<"loading delta-graph on: "<<current_shard();
		std::vector<std::tuple<K, ChangeEdgeType, D>> changes;
		std::string line;
		//read a line of the input file
		while(getline(inFile,line)){
			if(line.empty())
				continue;
			K key;
			ChangeEdgeType type;
			D data;
			maiter->iterkernel->read_change(line, key, type, data);
			// go without checking the type of changes
			changes.emplace_back(key, type, move(data));
		}
		return changes;
	}

	void apply_changes_on_graph(TypedGlobalTable<K, V, V, D>* table,
			std::vector<std::tuple<K, ChangeEdgeType, D>>& changes)
	{
		for(auto& tup : changes){
			const K& key =  std::get<0>(tup);
			const ChangeEdgeType type = std::get<1>(tup);
			const D& data = std::get<2>(tup);
			table->change_graph(key, type, data);
		}
	}

	// handle the delta information related with the graph changes
	void apply_changes_on_delta(TypedGlobalTable<K, V, V, D>* table,
			std::vector<std::tuple<K, ChangeEdgeType, D>>& changes)
	{
		// step 1: prepare messages (for remote nodes)
		std::vector<KVPairData> puts(table->num_shards());
		for(int i = 0; i < table->num_shards(); ++i){
			if(!table->is_local_shard(i)){
				KVPairData& put=puts[i];
				put.Clear();
				put.set_shard(i);
				put.set_source(table->helper()->id());
				put.set_table(table->id());
				put.set_epoch(table->helper()->epoch());
				put.set_done(true);
			}
		}
		// step 2: put changes into messages(remote) / apply(local)
		V default_v = maiter->iterkernel->default_v();
		string from, to, value;
		for(auto& tup : changes){
			K key = std::get<0>(tup);
			ChangeEdgeType type = std::get<1>(tup);
			K dst = maiter->iterkernel->get_keys(std::get<2>(tup)).front();
			V weight;
			if(type==ChangeEdgeType::REMOVE){
				weight = default_v;
			}else{
				ClutterRecord<K, V, V, D> c = table->get(key);
				std::vector<std::pair<K, V>> output;
				maiter->iterkernel->g_func(c.k, c.v1, c.v2, c.v3, &output);
				auto it = std::find_if(output.begin(), output.end(), [&](const std::pair<K, V>& p){
					return p.first==dst;
				});
				weight= it==output.end()? default_v : it->second;
			}
			VLOG(1)<<"  "<<char(type)<<" "<<key<<" "<<dst<<"\t"<<weight;
			D d=table->getF3(key);
			auto ii=std::find_if(d.begin(), d.end(), [&](const typename D::value_type &p){
				return p.end==dst;
			});
			VLOG(1)<<"  "<<char(type)<<" "<<key<<" "<<dst<<"\t"<<ii->weight<<"\t d="<<table->getF1(key)<<" v="<<table->getF2(key);
			table->accumulateF1(key, dst, weight);
		}
		//VLOG(1)<<"  going to send";
		// step 3: send messages
		for(int i = 0; i < table->num_shards(); ++i){
			KVPairData& put=puts[i];
			if(!table->is_local_shard(i)){
				VLOG(1)<<"sending graph change messages from "<<current_shard()<<" to "<<i<<", size="<<put.kv_data_size();
				if(put.kv_data_size() != 0){
					table->helper()->realSendUpdates(table->owner(i),put);
				}
			}
		}
		table->helper()->signalToSend();
		table->helper()->signalToProcess();
	}

	void delta_table(TypedGlobalTable<K, V, V, D>* a){
		std::vector<std::tuple<K, ChangeEdgeType, D>> changes = read_delta(a);
		VLOG(1)<<"number of delta edges on "<<current_shard()<<" is "<<changes.size();
		// change the topology
		VLOG(1)<<"change topology on: "<<current_shard();
		apply_changes_on_graph(a, changes);

		// change the delta values (for destinations of the affected edges)
		// CANNOT use the delta-table (local aggregation) for min/max accumulators
		// because only the best one can be sent out and the rest are permanently lost
		VLOG(1)<<"re-initialize delta value and in-neighbor information on: "<<current_shard();
		apply_changes_on_delta(a, changes);
		//corridnate_in_neighbors(a);
	}

	void run(){
		if(!FLAGS_delta_name.empty()){
			VLOG(0) << "load & apply delta graph on "<<current_shard();
			delta_table(maiter->table);
		}else{
			VLOG(0) << "skip loading delta graph on "<<current_shard();
		}
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
		// DLOG(INFO)<<"pending writes: "<<tgt->pending_send_;
	}

	void map(){
		VLOG(0) << "start performing iterative update";
		/*
		typename StateTable<K, V, V, D>::EntirePassIterator *it =
			dynamic_cast<typename StateTable<K, V, V, D>::EntirePassIterator*>(
					maiter->table->get_entirepass_iterator(current_shard()));
		std::ofstream fout(maiter->output+"/xxx");
		while(!it->done()){
			fout << it->key() << "\t" << it->value1()<<"\t"<<it->value2()<<"\t";
			const std::unordered_map<K, V>& ineigh = it->ineighbor();
			for(auto jt=ineigh.begin(); jt!=ineigh.end(); ++jt){
				fout<< jt->first <<','<< jt->second <<' ';
			}
			fout<<"\n";
			it->Next();
		}
		delete it;
		fout.close();
		 */
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
			// bool cont = it->Next();
			// if(!cont) break;

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

// dump the in-neighbor list together with the values on them
template<class K, class V, class D>
class MaiterKernelDumpInNeighbor: public DSMKernel{
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

		// load and apply the delta graph
		KernelRegistrationHelper<MaiterKernelLoadDeltaGraph<K, V, D>, K, V, D>("MaiterKernelLoadDeltaGraph", this);
		MethodRegistrationHelper<MaiterKernelLoadDeltaGraph<K, V, D>, K, V, D>("MaiterKernelLoadDeltaGraph", "run",
				&MaiterKernelLoadDeltaGraph<K, V, D>::run, this);

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

		// dump neighbor
		KernelRegistrationHelper<MaiterKernelDumpInNeighbor<K, V, D>, K, V, D>("MaiterKernelDumpInNeighbor", this);
		MethodRegistrationHelper<MaiterKernelDumpInNeighbor<K, V, D>, K, V, D>("MaiterKernelDumpInNeighbor", "run",
				&MaiterKernelDumpInNeighbor<K, V, D>::run, this);

		return 0;
	}
};

}
#endif /* KERNEL_H_ */
