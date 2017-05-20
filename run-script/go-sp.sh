ALGORITHM=Shortestpath
if [ $# -lt 1 ]; then
	echo 'folder is not griven'
	exit
fi
FOLDER=${1// /} # remove all spaces
# ${var/pattern/string} replace the FIRST occurence of the pattern with string.
# if pattern starts with '/' replace ALL matches of the pattern
HEAD=$(echo $FOLDER|sed -r 's/-[0-9]+$//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
#WORKERS=2
WORKERS=$(( $PARTS + 1 ))
DATA_PREFIX=/tmp/gdata
GRAPH=$DATA_PREFIX/input/$FOLDER
RESULT=$DATA_PREFIX/output/$FOLDER
NODES=20
SNAPSHOT=5
TERMTHRESH=0.000000001
BUFTIME=0.01
CP_TYPE=CP_NONE
CP_TIME=5
INIT_DIR=""
if [ $# -ge 2 ] && [ ! -z ${2// /} ]; then
	INIT_DIR=--init_dir=$DATA_PREFIX/$2
fi
DELTA_PREFIX=""
if [ $# -ge 3 ] && [ ! -z ${2// /} ]; then
	DELTA_PREFIX=--delta_prefix=$DATA_PREFIX/$3
	DELTA_NAME=$(basename $3)
	RESULT=$DATA_PREFIX/output/$FOLDER-$DELTA_NAME
fi
PORTION=1
if [ $# -ge 4 ]; then
	PORTION=$4
fi
ALPHA=1
if [ $# -ge 5 ]; then
	ALPHA=$5
	#HOSTFILE=$5
fi
HOSTFILE=../conf/maiter-cluster
if [ $# -ge 6 ]; then
	HOSTFILE=$6
fi

mkdir -p $RESULT

#echo $GRAPH  $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
 --graph_dir=$GRAPH --result_dir=$RESULT $INIT_DIR $DELTA_PREFIX\
 --local_aggregate=0 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
 --sleep_time=0.01 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME\
 --v=0 --weight_alpha=$ALPHA --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME

