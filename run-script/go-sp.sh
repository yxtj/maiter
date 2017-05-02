ALGORITHM=Shortestpath
if [ $# -lt 1 ]; then
	echo 'folder is not griven'
	exit
fi
FOLDER=$1
HEAD=$(echo $FOLDER|sed -r 's/-[0-9]+//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
WORKERS=$(( $PARTS + 1 ))
DATA_PREFIX=../test
GRAPH=$DATA_PREFIX/input/$FOLDER
RESULT=$DATA_PREFIX/output/$FOLDER
NODES=20
SNAPSHOT=5
TERMTHRESH=0.000000001
BUFTIME=0.1
CP_TYPE=CP_NONE
CP_TIME=5
INIT_DIR=""
if [ $# -ge 2 ]; then
	INIT_DIR=--init_dir=$DATA_PREFIX/$2
fi
DELTA_NAME=""
if [ $# -ge 3 ]; then
	DELTA_NAME=--delta_name=$3
	RESULT=$DATA_PREFIX/output/$HEAD-$3-$PARTS
fi
HOSTFILE=../conf/maiter-cluster
PORTION=1
if [ $# -ge 4 ]; then
	PORTION=$4
fi
if [ $# -ge 5 ]; then
	HOSTFILE=$5
fi

mkdir -p $RESULT

#echo $GRAPH  $HOSTFILE

../maiter --hostfile=$HOSTFILE --runner=$ALGORITHM --workers=$WORKERS --num_nodes=$NODES\
  --graph_dir=$GRAPH --result_dir=$RESULT $INIT_DIR $DELTA_NAME\
  --local_aggregate=0 --snapshot_interval=$SNAPSHOT --portion=$PORTION\
  --sleep_time=0.01 --termcheck_threshold=$TERMTHRESH --buftime=$BUFTIME\
  --checkpoint_type=$CP_TYPE --checkpoint_interval=$CP_TIME --v=1 

