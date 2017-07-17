if [ $# -lt 3 ] || [ $# -gt 10 ] ; then
	echo 'Usage: <algorithm> <prefix> <graph-fdr> [delta-prefix] [portion] [alpha] [degree] [snapshot] [verbose] [hostfile]'
	echo '  <algorithm>: the algorithm to run'
	echo '  <graph-fdf>: should follow the format "xxx-n", where "n" is the number of parts'
	echo '  graph : <prefix>/input/<graph-fdr>'
	echo '  result: <prefix>/output/<graph-fdr>'
	echo '  init  : <prefix>/ref/<graph-fdr>'
	echo '  delta : <prefix>/delta/<graph-fdr>/<delta-prefix>'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [alpha]: (=1) the weight for the bad changes'
	echo '  [degree]: (=0) use degree in setting priority'
	echo '  [snapshot]: (=1) termination checking interval, in seconds'
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
BUFTIME=0.003
CP_TYPE=CP_NONE
CP_TIME=5

PORTION=1
if [ $# -ge 5 ] && [ ! -z ${5// /} ]; then
	PORTION=$5
fi
ALPHA=1
if [ $# -ge 6 ] && [ ! -z ${6// /} ]; then
	ALPHA=$6
fi
DEGREE=0
if [ $# -ge 7 ] && [ ! -z ${7// /} ]; then
	DEGREE=$7
fi
SNAPSHOT=1
if [ $# -ge 8 ] && [ ! -z ${8// /} ]; then
	SNAPSHOT=$8
fi
VERBOSE_LVL=0
if [ $# -ge 9 ] && [ ! -z ${9// /} ]; then
	VERBOSE_LVL=$9
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 10 ] && [ ! -z ${10// /} ]; then
	HOSTFILE=${10}
fi

mkdir -p $RESULT

#echo $GRAPH  $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
 --graph_dir=$GRAPH --result_dir=$RESULT $INIT_DIR $DELTA_PREFIX\
 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
 --sleep_time=0.003 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME --v=$VERBOSE_LVL\
 --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME\
 --weight_alpha=$ALPHA --priority_degree=$DEGREE --local_aggregate=$LOCAL_AGG 

