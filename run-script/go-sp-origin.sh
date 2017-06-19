ALGORITHM=ShortestPath
if [ $# -lt 3 ] || [ $# -gt 8 ] ; then
	echo 'Usage: <#-parts> <graph-fdr> <result-fdr> [portion] [alpha] [degree] [snapshot] [verbose] [hostfile]'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [alpha]: (=1) the priority weight for the bad changes'
	echo '  [degree]: (=0) use degree in setting priority'
	echo '  [snapshot]: (=1) termination checking interval, in seconds'
	echo '  [verbose]: (=0) verbose level, the higher the more verbose'
	echo '  [hostfile]: (=../conf/maiter-cluster) the hostfile for MPI'
	exit
fi

PARTS=$1
WORKERS=$(( $PARTS + 1 ))

GRAPH=$2
RESULT=$3

NODES=100
TERMTHRESH=0.000000001
BUFTIME=0.003
CP_TYPE=CP_NONE
CP_TIME=5

PORTION=1
if [ $# -ge 4 ]; then
	PORTION=$4
fi
ALPHA=1
if [ $# -ge 5 ]; then
	ALPHA=$5
fi
DEGREE=0
if [ $# -ge 6 ]; then
	DEGREE=$6
fi
SNAPSHOT=1
if [ $# -ge 7 ]; then
	SNAPSHOT=$7
fi
VERBOSE_LVL=0
if [ $# -ge 8 ]; then
	VERBOSE_LVL=$8
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 9 ]; then
	HOSTFILE=$9
fi

mkdir -p $RESULT

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
 --graph_dir=$GRAPH --result_dir=$RESULT --local_aggregate=0 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
 --sleep_time=0.01 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME\
 --v=$VERBOSE_LVL --weight_alpha=$ALPHA --priority_degree=$DEGREE --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME

