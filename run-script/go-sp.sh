ALGORITHM=Shortestpath
if [ $# -lt 2 ] || [ $# -gt 8 ] ; then
	echo 'Usage: <prefix> <graph-fdr> [delta-prefix] [portion] [alpha] [snapshot] [verbose] [hostfile]'
	echo '  graph : <prefix>/input/<graph-fdr>'
	echo '  result: <prefix>/output/<graph-fdr>'
	echo '  init  : <prefix>/ref/<graph-fdr>'
	echo '  delta : <prefix>/delta/<graph-fdr>/<delta-prefix>'
	echo '  [portion]: (=1) the top portion for priority scheduling, 1 means Round-Robin'
	echo '  [alpha]: (=1) the weight for the bad changes'
	echo '  [snapshot]: (=3) termination checking interval, in seconds'
	echo '  [verbose]: (=0) verbose level, the higher the more verbose'
	echo '  [hostfile]: (=../conf/maiter-cluster) the hostfile for MPI'
	exit
fi

DATA_PREFIX=${1}
FOLDER=${2// /} # remove all spaces
# ${var/pattern/string} replace the FIRST occurence of the pattern with string.
# if pattern starts with '/' replace ALL matches of the pattern
HEAD=$(echo $FOLDER | sed -r 's/-[0-9]+$//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
WORKERS=$(( $PARTS + 1 ))
GRAPH=$DATA_PREFIX/input/$FOLDER
RESULT=$DATA_PREFIX/output/$FOLDER

INIT_DIR=""
DELTA_PREFIX=""
if [ $# -ge 3 ] && [ ! -z ${3// /} ]; then
	INIT_DIR=--init_dir=$DATA_PREFIX/ref/$FOLDER
	DELTA_PREFIX=--delta_prefix=$DATA_PREFIX/delta/$FOLDER/$3
	DELTA_NAME=$(basename $3)
#	RESULT=$DATA_PREFIX/output/$HEAD-$DELTA_NAME-$PARTS
	RESULT=$DATA_PREFIX/output/$FOLDER-$DELTA_NAME
fi

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
SNAPSHOT=3
if [ $# -ge 6 ]; then
	SNAPSHOT=$6
fi
VERBOSE_LVL=0
if [ $# -ge 7 ]; then
	VERBOSE_LVL=$7
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 8 ]; then
	HOSTFILE=$8
fi

mkdir -p $RESULT

#echo $GRAPH  $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
 --graph_dir=$GRAPH --result_dir=$RESULT $INIT_DIR $DELTA_PREFIX\
 --local_aggregate=0 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
 --sleep_time=0.01 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME\
 --v=$VERBOSE_LVL --weight_alpha=$ALPHA --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME

