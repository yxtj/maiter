ALGORITHM=Shortestpath
if [ $# -lt 5 ]; then
	echo 'Usage: <#-parts> <graph-fdr> <init-fdr> <delta-prefix> <result-fdr> [portion] [alpha] [snapshot] [verbose] [hostfile]'
	echo '  <#-parts>: # of parts'
	echo '  <*-fdr> = the full path of the folders'
	echo '  <delta-prefix> = the folder and name prefix for delta graphs, "-<part-id>" is added for each parts'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [alpha]: (=1) the fixed weight for the bad changes'
	echo '  [snapshot]: (=1) termination checking interval, in seconds'
	echo '  [verbose]: (=0) verbose level, the higher the more verbose'
	echo '  [hostfile]: (=../conf/maiter-cluster) the hostfile for MPI'
	exit
fi

PARTS=$1
WORKERS=$(( $PARTS + 1 ))

GRAPH_FDR=$2
INIT_FDR=$3
DELTA_PRE=$4
RESULT_FDR=$5

NODES=1000000
TERMTHRESH=0.000000001
BUFTIME=0.1
CP_TYPE=CP_NONE
CP_TIME=5

PORTION=1
if [ $# -ge 6 ]; then
	PORTION=$6
fi
ALPHA=1
if [ $# -ge 7 ]; then
	ALPHA=$7
fi
SNAPSHOT=1
if [ $# -ge 8 ]; then
	SNAPSHOT=$8
fi

VERBOSE_LVL=0
if [ $# -ge 9 ]; then
	VERBOSE_LVL=$9
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 10 ]; then
	HOSTFILE=${10}
fi

mkdir -p $RESULT

#echo $GRAPH_FDR $HOSTFILE

../maiter --hostfile=../conf/maiter-cluster --runner=Shortestpath --workers=$WORKERS --num_nodes=$NODES\
  --graph_dir=$GRAPH_FDR --result_dir=$RESULT_FDR --init_dir=$INIT_FDR --delta_prefix=$DELTA_PRE\
  --local_aggregate=0 --snapshot_interval=$SNAPSHOT --portion=$PORTION --weight_alpha=$ALPHA\
  --sleep_time=0.01 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME -- v=$VERBOSE_LVL
#  --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME

