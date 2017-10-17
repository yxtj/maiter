if [ $# -lt 3 ] || [ $# -gt 14 ] ; then
	echo 'Usage: <algorithm> <prefix> <graph-fdr> [delta-prefix] [portion] [diff] [alpha] [degree] [snapshot] [buftime] [net-ratio] [net-delay] [verbose] [hostfile]'
	echo '  <algorithm>: the algorithm to run'
	echo '  <graph-fdf>: should follow the format "xxx-n", where "n" is the number of parts'
	echo '  graph : <prefix>/input/<graph-fdr>'
	echo '  result: <prefix>/output/<graph-fdr>'
	echo '  init  : <prefix>/ref/<graph-fdr>'
	echo '  delta : <prefix>/delta/<graph-fdr>/<delta-prefix>'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [diff]: (=0) use difference-based or value-based priority'
	echo '  [alpha]: (=1) the weight for the bad changes'
	echo '  [degree]: (=0) use degree in setting priority'
	echo '  [snapshot]: (=1) termination checking interval, in seconds'
	echo '  [buftime]: (=0.003) time for buffering outgoing messages'
	echo '  [net-ratio]: (=inf) the sending ratio of each worker (bytes per second)'
	echo '  [net-delay]: (=0.0) delay before commit a received message (in second)'
	echo '  [verbose]: (=0) verbose level, the higher the more verbose'
	echo '  [hostfile]: (=../conf/maiter-cluster) the hostfile for MPI'
	exit
fi

ALGORITHM=${1}
LOCAL_AGG=$(./do_local_aggregation.sh $ALGORITHM)
if [ $? == 1 ]; then
	echo "error calling do_local_aggregation.sh"
	exit 1
fi

DATA_PREFIX=${2}
FOLDER=${3// /} # remove all spaces
# ${var/pattern/string} replace the FIRST occurence of the pattern with string.
# if pattern starts with '/' replace ALL matches of the pattern
HEAD=$(echo $FOLDER | sed -r 's/-[0-9]+$//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
WORKERS=$(( $PARTS + 1 ))
GRAPH=$DATA_PREFIX/input/$FOLDER
RESULT=$DATA_PREFIX/output/$FOLDER

INIT_DIR=""
DELTA_PREFIX=""
if [ $# -ge 4 ] && [ ! -z ${4// /} ]; then
	INIT_DIR=--init_dir=$DATA_PREFIX/ref/$FOLDER
	DELTA_PREFIX=--delta_prefix=$DATA_PREFIX/delta/$FOLDER/$4
	DELTA_NAME=$(basename $4)
#	RESULT=$DATA_PREFIX/output/$HEAD-$DELTA_NAME-$PARTS
	RESULT=$DATA_PREFIX/output/$FOLDER-$DELTA_NAME
fi

NODES=100
TERMTHRESH=0.000000001
CP_TYPE=CP_NONE
CP_TIME=5

PORTION=1
if [ $# -ge 5 ] && [ ! -z ${5// /} ]; then
	PORTION=$5
fi
DIFF=0
if [ $# -ge 6 ] && [ ! -z ${6// /} ]; then
	DIFF=$6
fi
ALPHA=1
if [ $# -ge 7 ] && [ ! -z ${7// /} ]; then
	ALPHA=$7
fi
DEGREE=0
if [ $# -ge 8 ] && [ ! -z ${8// /} ]; then
	DEGREE=$8
fi
SNAPSHOT=1
if [ $# -ge 9 ] && [ ! -z ${9// /} ]; then
	SNAPSHOT=$9
fi
BUFTIME=0.003
if [ $# -ge 10 ] && [ ! -z ${10// /} ]; then
	BUFTIME=${10}
fi
NET_RATIO=inf
if [ $# -ge 11 ] && [ ! -z ${11// /} ]; then
	NET_RATIO=${11}
fi
NET_DELAY=0
if [ $# -ge 12 ] && [ ! -z ${12// /} ]; then
	NET_DELAY=${12}
fi
VERBOSE_LVL=0
if [ $# -ge 13 ] && [ ! -z ${13// /} ]; then
	VERBOSE_LVL=${13}
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 14 ] && [ ! -z ${14// /} ]; then
	HOSTFILE=${14}
fi

mkdir -p $RESULT

#echo $GRAPH  $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
 --graph_dir=$GRAPH --result_dir=$RESULT $INIT_DIR $DELTA_PREFIX\
 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
 --net_ratio=$NET_RATIO --net_delay_time=$NET_DELAY\
 --sleep_time=0.003 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME --v=$VERBOSE_LVL\
 --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME\
 --priority_diff=$DIFF --weight_alpha=$ALPHA --priority_degree=$DEGREE --local_aggregate=$LOCAL_AGG 

