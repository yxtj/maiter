#include "client/client.h"

using namespace dsm;



template <class K, class V, class D>
class MaiterKernel{
    
public:
        
    int64_t num_nodes;
    double schedule_portion;
    ConfigData conf;
    string output;
    Sharder<K> *sharder;
    Initializer<K, V, D> *initializer;
    Accumulator<V> *accum;
    Sender<K, V, D> *sender;
    TermChecker<K, V> *termchecker;

    TypedGlobalTable<K, V, V, D> *table;

    
    MaiterKernel() { Reset(); }

    MaiterKernel(ConfigData& inconf, int64_t nodes, double portion, string outdir,
                    Sharder<K>* insharder,
                    Initializer<K, V, D>* ininitializer,
                    Accumulator<V>* inaccumulator,
                    Sender<K, V, D>* insender,
                    TermChecker<K, V>* intermchecker) {
        Reset();
        
        conf = inconf;
        num_nodes = nodes;
        schedule_portion = portion;
        output = outdir;
        sharder = insharder;
        initializer = ininitializer;
        accum = inaccumulator;
        sender = insender;
        termchecker = intermchecker;
    }
    
    ~MaiterKernel(){}


    void Reset() {
        num_nodes = 0;
        schedule_portion = 1;
        output = "result";
        sharder = NULL;
        initializer = NULL;
        accum = NULL;
        sender = NULL;
        termchecker = NULL;
    }
 
    class MaiterKernel1 : public DSMKernel {
    private:
        MaiterKernel<K, V, D>* maiter;
    public:
        MaiterKernel1(){
            maiter = NULL;
        }
        
        void set_maiter(MaiterKernel* inmaiter){
            maiter = inmaiter;
        }
        
        void init_table(TypedGlobalTable<K, V, V, D>* a){
            if(!a->initialized()){
                VLOG(0) << "init table2";
                a->InitStateTable();
            }
            VLOG(0) << "init table3";
            a->resize(maiter->num_nodes);
            VLOG(0) << "init table4";
            maiter->initializer->initTable(a, current_shard());
        }

        void run(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
            VLOG(0) << "init table ";
            init_table(maiter->table);
        }
    };

    class MaiterKernel2 : public DSMKernel, MaiterKernel {
    private:
        MaiterKernel<K, V, D>* maiter;
        vector<pair<K, V> >* output;
        int threshold;
        
    public:
        void run_iter(const K& k, V &v1, V &v2, D &v3) {
            maiter->table->accumulateF2(k, v1);
            
            maiter->sender->send(v1, v3, output);
            if(output->size() > threshold){
                typename vector<pair<K, V> >::iterator iter;
                for(iter = output->begin(); iter != output->end(); iter++) {
                    pair<K, V> kvpair = *iter;
                    maiter->table->accumulateF1(kvpair.first, kvpair.second);
                }
                output->clear();
            }
             
            maiter->table->updateF1(k, sender->reset(k, v1));
        }

        void run_loop(TypedGlobalTable<K, V, V, D>* a) {
            Timer timer;
            double totalF1 = 0;
            int updates = 0;
            output = new vector<pair<K, V> >;
            threshold = 1000;
            
            //the main loop for iterative update
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

        void map(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
            VLOG(0) << "start iterative update";
            run_loop(maiter->table);
        }
    };

    class MaiterKernel3 : public DSMKernel, MaiterKernel {
    private:
        MaiterKernel<K, V, D>* maiter;
    public:
        void dump(TypedGlobalTable<K, V, V, D>* a){
            double totalF1 = 0;
            double totalF2 = 0;
            fstream File;

            string file = StringPrintf("%s/part-%d", output.c_str(), current_shard());
            File.open(file.c_str(), ios::out);

            /*
            typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_typed_iterator(current_shard(), true);

            for (; !it->done(); it->Next()) {
                    totalF1 += it->value1();
                    totalF2 += it->value2();
                    File << it->key() << "\t" << it->value1() << "|" << it->value2() << "\n";
            }
            delete it;
            */       

            for (int i = current_shard(); i < num_nodes; i += conf.num_workers()) {
                    totalF1 += maiter->table->getF1(i);
                    totalF2 += maiter->table->getF2(i);

                    File << i << "\t" << maiter->table->getF1(i) << "|" << maiter->table->getF2(i) << "\n";
            };


            File.close();

            cout << "total F1 : " << totalF1 << endl;
            cout << "total F2 : " << totalF2 << endl;
        }

        void run(MaiterKernel<K, V, D>* inmaiter) {
            maiter = inmaiter;
            VLOG(0) << "dump result";
            dump(maiter->table);
        }
    };


public:
    int run() {
	VLOG(0) << "shards " << conf.num_workers();
	table = CreateTable<K, V, V, D >(0, conf.num_workers(), schedule_portion,
                                        sharder, accum, termchecker);
                
        KernelRegistrationHelper<MaiterKernel1, K, V, D>("MaiterKernel1", this);
        MethodRegistrationHelper<MaiterKernel1, K, V, D>("MaiterKernel1", "run", &MaiterKernel1::run, this);

        KernelRegistrationHelper<MaiterKernel2, K, V, D>("MaiterKernel2", this);
        MethodRegistrationHelper<MaiterKernel2, K, V, D>("MaiterKernel2", "map", &MaiterKernel2::map, this);

        KernelRegistrationHelper<MaiterKernel3, K, V, D>("MaiterKernel3", this);
        MethodRegistrationHelper<MaiterKernel3, K, V, D>("MaiterKernel3", "run", &MaiterKernel3::run, this);
                
	if (!StartWorker(conf)) {
            Master m(conf);
            m.run_all("MaiterKernel1", "run", table);
            m.run_all("MaiterKernel2", "map", table);
            m.run_all("MaiterKernel3", "run", table);
	}
	return 0;
    }
};
