/*
 * maiter-kernel.h
 *
 *  Created on: May 6, 2017
 *      Author: tzhou
 */

#ifndef KERNEL_MAITER_KERNEL_H_
#define KERNEL_MAITER_KERNEL_H_

#include "kernel.h"
#include "DSMKernel.h"
#include "table/table-registry.h"
#include "table/typed-global-table.hpp"
#include "table/table-create.h"
#include "table/tbl_widget/IterateKernel.h"
#include "util/timer.h"
#include <string>
#include <fstream>

DECLARE_string(graph_dir);
DECLARE_string(init_dir);
DECLARE_string(delta_prefix);
DECLARE_double(sleep_time);


namespace dsm{

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
		std::string patition_file = FLAGS_graph_dir+"/part"+std::to_string(current_shard());
		std::ifstream inFile(patition_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open graph file: " << patition_file;
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
		std::string init_file = FLAGS_init_dir+"/part-"+std::to_string(current_shard());
		std::ifstream inFile(init_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open initializing file: " << init_file;
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
			// format: "<key>\t<delta>:<value>"
			maiter->iterkernel->read_init(line, key, delta, value);
			if(use_initial_delta){
				D d = table->getF3(key);
				maiter->iterkernel->init_c(key, delta, d);
			}else if(use_v_for_delta){
				// For min/max case, delta should be set to value
				//VLOG(1)<<"load "<<key<<" delta: "<<delta<<" value: "<<value;
				delta = value;
			}
			table->updateF1(key, delta);
			table->updateF2(key, value);
		}
	}

	void coordinate_in_neighbors(TypedGlobalTable<K, V, V, D>* table, const bool non_default_in_neighbor){
		// whether to send processed (g_func) delta to out-neighbors
		table->fill_ineighbor_cache(non_default_in_neighbor);
		table->apply_inneighbor_cache_local();
		table->send_ineighbor_cache_remote();
		table->clear_ineighbor_cache();
		table->reset_ineighbor_bp(); // reset bp first GZZZ
	}
	void init_table(TypedGlobalTable<K, V, V, D>* a){
		if(!a->initialized()){
			a->InitStateTable();        //initialize the local state table
		}
		a->resize(maiter->num_nodes);   //create local state table based on the input size

		// step 1: load graph
		VLOG(0)<<"loading graphs on "<<current_shard();
		read_file(a);               //initialize the state table fields based on the input data file

		// step 2: load initial value & delta
		bool load_initial_value=!FLAGS_init_dir.empty();
		bool is_selective = maiter->iterkernel->is_selective();
		VLOG(1)<<"load_in_value "<<load_initial_value<<" selective? "<<is_selective;
		if(load_initial_value){
			VLOG(0)<<"loading initial values on "<<current_shard();
			load_initial(a, false, is_selective);
		}

		// step 3: coordinate in-neighbors
		coord();
	}

	void coord(){
		VLOG(0) << "building up in-neighbor list on "<<current_shard();
		bool load_initial_value=!FLAGS_init_dir.empty();
		bool is_selective = maiter->iterkernel->is_selective();
		coordinate_in_neighbors(maiter->table, load_initial_value && is_selective);
	}

	void run(){
		VLOG(0) << "initializing table on "<<current_shard();
		init_table(maiter->table);
		VLOG(0) << "table initialized on "<<current_shard();
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
		std::string patition_file = FLAGS_delta_prefix+"-"+std::to_string(current_shard());
		std::ifstream inFile(patition_file);
		if(!inFile){
			LOG(FATAL) << "Unable to open delta file: " << patition_file;
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

	// local change
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

	// remote change (handle the delta information affected by the graph changes)
	void apply_changes_on_delta(TypedGlobalTable<K, V, V, D>* table,
			std::vector<std::tuple<K, ChangeEdgeType, D>>& changes)
	{
		double t1=0, t2=0, t3=0, t4=0;
		// put changes into messages(remote) / apply(local)
		V default_v = maiter->iterkernel->default_v();
		string from, to, value;
		Timer tmr;
		for(auto& tup : changes){
			K key = std::get<0>(tup);
			ChangeEdgeType type = std::get<1>(tup);
			K dst = maiter->iterkernel->get_keys(std::get<2>(tup)).front();
			V weight;
			if(type==ChangeEdgeType::REMOVE){
				weight = default_v;
			}else{
				tmr.reset();
				ClutterRecord<K, V, V, D> c = table->get(key);
				t1+=tmr.elapsed();
				tmr.reset();
				weight = maiter->iterkernel->g_func(c.k, c.v1, c.v2, c.v3, dst);
				t2+=tmr.elapsed();
			}
//			VLOG(1)<<"  "<<char(type)<<" "<<key<<" "<<dst<<"\t"<<weight<<"\t d="<<table->getF1(key)<<" v="<<table->getF2(key);
			tmr.reset();
			table->accumulateF1(key, dst, weight);
			t3+=tmr.elapsed();
			tmr.reset();
			if(table->canSend()){
				table->helper()->signalToSend();
				table->resetSendMarker();
			}
			t4+=tmr.elapsed();
		}
//		table->helper()->signalToProcess();
		table->helper()->signalToSend();
		table->resetSendMarker();
		VLOG(0)<<t1<<" , "<<t2<<" , "<<t3<<" , "<<t4;
	}

	void delta_table(TypedGlobalTable<K, V, V, D>* a){
		Timer tmr;
		std::vector<std::tuple<K, ChangeEdgeType, D>> changes = read_delta(a);
		VLOG(1)<<"load "<<changes.size()<<" delta edges in "<<tmr.elapsed()<<" on "<<current_shard();
		tmr.reset();

		// change the topology
		apply_changes_on_graph(a, changes);
		VLOG(1)<<"change topology on: "<<current_shard()<<" in "<<tmr.elapsed();
		tmr.reset();

		// change the delta values (for destinations of the affected edges)
		// CANNOT use the delta-table (local aggregation) for min/max accumulators
		// because only the best one can be sent out and the rest are permanently lost
		apply_changes_on_delta(a, changes);
		VLOG(1)<<"re-initialize delta value and in-neighbor information on: "<<current_shard()<<" in "<<tmr.elapsed();
		//corridnate_in_neighbors(a);
	}

	void run(){
		if(!FLAGS_delta_prefix.empty()){
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
//					tgt->resetProcessMarker();
				}
				if(tgt->canSend()){
					tgt->helper()->signalToSend();
//					tgt->resetSendMarker();
				}
//				tgt->helper()->signalToProcess();
//				tgt->helper()->signalToSend();
			}
			if(tgt->canTermCheck())
				tgt->helper()->signalToTermCheck();
			//std::this_thread::sleep_for(std::chrono::duration<double>(FLAGS_sleep_time));
		}
		// DLOG(INFO)<<"pending writes: "<<tgt->pending_send_;
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
		V totalF1 = 0;	//the sum of delta_v, it should be smaller enough when iteration converges
		uint64_t ndefF1 = 0;
		V totalF2 = 0;	//the sum of v, it should be larger enough when iteration converges
		uint64_t ndefF2 = 0;
		V default_v = maiter->iterkernel->default_v();

		std::string file =  maiter->output + "/part-" + std::to_string(current_shard());//the output path
		std::ofstream File(file);	//the output file containing the local state table infomation

		//get the iterator of the local state table
		typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());

		while(!it->done()){
			double t1=it->value1();
			double t2=it->value2();
			if(t1!=default_v)
				totalF1 +=t1;
			else
				++ndefF1;
			if(t2!=default_v)
				totalF2 +=t2;
			else
				++ndefF2;
			File << it->key() << "\t" << it->value1() << ":" << it->value2() << "\n";
			it->Next();
		}
		delete it;

		File.close();
		VLOG(0) << "W" << maiter->conf.worker_id()
				<< ":\ttotal F1: (" << totalF1 << ", " << ndefF1 << ")"
				<< "\ttotal F2: (" << totalF2 << ", " << ndefF2 << ")";
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
		std::string file = maiter->output + "/ilist-" + std::to_string(current_shard());
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
		table = CreateTable<K, V, V, D>(0, conf.num_workers(), schedule_portion, sharder,
				iterkernel, termchecker);

		//initialize table job
		KernelRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", this);
		MethodRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", "run",
				&MaiterKernel1<K, V, D>::run, this);

		// build up the in-neighbor list
		MethodRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", "coord",
				&MaiterKernel1<K, V, D>::coord, this);

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

} //namespace

#endif /* KERNEL_MAITER_KERNEL_H_ */
