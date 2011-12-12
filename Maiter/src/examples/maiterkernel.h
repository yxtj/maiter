#include "client/client.h"


using namespace dsm;

template <class K, class V, class D>
static TypedGlobalTable<K, V, V, D> *table;

template <class K, class V, class D>
static MaiterKernel<K, V, D> *maiterkernel;

template <class K, class V>
static void sendto(K& k, V& v){
	table->accumulateF1(k, v);
}

template <class K, class V, class D>
struct MaiterKernel {
	public:
	MaiterKernel() { Reset(); }

	MaiterKernel(ConfigData& inconf, double portion, string outdir,
			Sharder<K>* insharder,
			Initializer<K, V, D>* ininitializer
			Accumulator<V>* inaccumulator,
			Updater<K, V, D>* insender,
			TermChecker<K, V>* intermchecker) {
		Reset();
		conf = inconf;
		schedule_portion = portion;
		output = outdir;
		sharder = insharder;
		initializer = ininitializer;
		accum = inaccumulator;
		sender = insender;
		termchecker = intermchecker;
	}

	void Reset() {
		conf = NULL;
		schedule_portion = 1;
		output = "result";
		sharder = NULL;
		initializer = NULL;
		accum = NULL;
		sender = NULL;
		termchecker = NULL;
	}

	double schedule_portion;
	ConfigData& conf;
	string output;
	Sharder<K> *sharder;
	Initializer<K, V, D> *initializer;
	Accumulator<V> *accum;
	Sender<K, V, D> *sender;
	TermChecker<K, V> *termchecker;
};

/*
static int MaiterRunner(ConfigData& conf) {
	int shards = conf.num_workers();
	VLOG(0) << "shards " << shards;
  table = CreateTable<int, float, float, vector<int> >(0, shards, FLAGS_portion, new Sharding::Mod,
		  new Accumulators<float>::Sum, new PagerankTermChecker);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("MaiterKernel1", "run", table);
    m.run_all("MaiterKernel2", "map", table);
    m.run_all("MaiterKernel3", "run", table);
  }
  return 0;
}
REGISTER_RUNNER(MaiterRunner);
*/

template <class K, class V, class D>
int run_maiter(MaiterKernel<K, V, D>* inmaiterkernel) {
	maiterkernel = inmaiterkernel;
	int shards = maiterkernel->conf.num_workers();
	VLOG(0) << "shards " << shards;
	table = CreateTable<K, V, V, D >(0, shards, maiterkernel->schedule_portion,
	maiterkernel->sharder, maiterkernel->accum, maiterkernel->termchecker);

	if (!StartWorker(*(maiterkernel->conf))) {
		Master m(maiterkernel->conf);
		m.run_all("MaiterKernel1", "run", table);
		m.run_all("MaiterKernel2", "map", table);
		m.run_all("MaiterKernel3", "run", table);
	}
	return 0;
}


class MaiterKernel1 : public DSMKernel {
public:
	template <class K, class V, class D>
	void init_table(TypedGlobalTable<K, V, V, D>* a){
		maiterkernel->initializer->initTable(a, current_shard());
	}

	void run() {
		VLOG(0) << "init table";
		init_table(pages);
	}
};

REGISTER_KERNEL(MaiterKernel1);
REGISTER_METHOD(MaiterKernel1, run);


class MaiterKernel2 : public DSMKernel {
public:
	template <class K, class V, class D>
	void run_iter(const K& k, V &v1, V &v2, D &v3) {
		table->accumulateF2(k, v1);
		maiterkernel->sender->send(v1, v3);
		table->updateF1(k, maiterkernel->sender->reset(k, v1));
	}

	template <class K, class V, class D>
	void run_loop(TypedGlobalTable<K, V, V, D>* a) {
		Timer timer;
		double totalF1 = 0;
		int updates = 0;
		while(true){
			typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_typed_iterator(current_shard(), true);
			if(it == NULL) break;

			for (; !it->done(); it->Next()) {
				totalF1+=it->value1();
				updates++;

				run_iter(it->key(), it->value1(), it->value2(), it->value3());
			}
			delete it;

			//for expr
			cout << timer.elapsed() << "\t" << current_shard() << "\t" << totalF1 << "\t" << updates << endl;
		}
	}

	void map() {
		VLOG(0) << "start iterative update";
		run_loop(table);
	}
};

REGISTER_KERNEL(MaiterKernel2);
REGISTER_METHOD(MaiterKernel2, map);

class MaiterKernel3 : public DSMKernel {
public:
	template <class K, class V, class D>
	void dump(TypedGlobalTable<K, V, V, D>* a){
		double totalF1 = 0;
		double totalF2 = 0;
		fstream File;

		string file = StringPrintf("%s/part-%d", maiterkernel->output.c_str(), current_shard());
		File.open(file.c_str(), ios::out);

		const int num_shards = table->num_shards();
		for (int i = current_shard(); i < FLAGS_num_nodes; i += num_shards) {
			totalF1 += table->getF1(i);
			totalF2 += table->getF2(i);

			File << i << "\t" << table->getF1(i) << "|" << table->getF2(i) << "\n";
		};

		File.close();

		cout << "total F1 : " << totalF1 << endl;
		cout << "total F2 : " << totalF2 << endl;
	}

	void run() {
		VLOG(0) << "dump result";
		dump(table);
	}
};

REGISTER_KERNEL(MaiterKernel3);
REGISTER_METHOD(MaiterKernel3, run);

