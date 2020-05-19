#ALGORITHM=ShortestPath
if [ $# -lt 6 ]; then
	echo 'Usage: <algorithm> <#-parts> <graph-fdr> <init-fdr> <delta-prefix> <result-fdr> [portion] [alpha] [degree] [snapshot] [verbose] [hostfile]'
	echo '  <algorithm>: the algorithm to run (i.e. ShortestPath, Pagerank, WidestPath, ConnectedComp)'
	echo '  <#-parts>: # of parts'
	echo '  <*-fdr> = the full path of the folders'
	echo '  <delta-prefix> = the folder and name prefix for delta graphs, "-<part-id>" is added for each parts'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [diff]: (=0) whether the priority is difference-based or value-based'
	echo '  [alpha]: (=1) the weight for the bad changes'
	echo '  [degree]: (=0) use degree in setting priority'
	echo '  [snapshot]: (=1) termination checking interval, in seconds'
	echo '  [verbose]: (=0) verbose level, the higher the more verbose'
	echo '  [hostfile]: (=../conf/maiter-cluster) the hostfile for MPI'
	exit
fi

ALGORITHM=$1
LOCAL_AGG=$(./do_local_aggregation.sh $ALGORITHM)
if [ $? == 1 ]; then
	echo "error in calling do_local_aggregation.sh"
	exit
fi

PARTS=$2
WORKERS=$(( $PARTS + 1 ))

GRAPH_FDR=$3
INIT_FDR=$4
DELTA_PRE=$5
RESULT_FDR=$6

NODES=1000000
TERMTHRESH=0.000000001
BUFTIME=0.005
CP_TYPE=CP_NONE
CP_TIME=5

PORTION=1
if [ $# -ge 7 ]; then
	PORTION=$7
fi
DIFF=0
if [ $# -ge 8 ]; then
	DIFF=$8
fi
ALPHA=1
if [ $# -ge 9 ]; then
	ALPHA=$9
fi
DEGREE=0
if [ $# -ge 10 ]; then
	DEGREE=${10}
fi
SNAPSHOT=1
if [ $# -ge 11 ]; then
	SNAPSHOT=${11}
fi

VERBOSE_LVL=0
if [ $# -ge 12 ]; then
	VERBOSE_LVL=${12}
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 13 ]; then
	HOSTFILE=${13}
fi

mkdir -p $RESULT_FDR

#echo $GRAPH_FDR $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
  --graph_dir=$GRAPH_FDR --result_dir=$RESULT_FDR --init_dir=$INIT_FDR --delta_prefix=$DELTA_PRE\
  --local_aggregate=$LOCAL_AGG --portion=$PORTION --priority_diff=$DIFF --weight_alpha=$ALPHA --priority_degree=$DEGREE\
  --snapshot_interval=$SNAPSHOT --sleep_time=0.003 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME --v=$VERBOSE_LVL
#  --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME

