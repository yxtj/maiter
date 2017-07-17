DELTA_NAME=top
# relationship between top-portion and runtime
if [ $# -lt 4 ]; then
	echo "Usage: <prefix> <graph-name> <k-start> <k-end> <log-sub-fdr> [degree] [snapshot]"
	echo '  File structure:'
	echo '    graph-folder        = <prefix>/input/<graph-name>'
	echo '    initializing-folder = <prefix>/ref/<graph-name>'
	echo "    delta-folder        = <prefix>/delta/<graph-name>/$DELTA_NAME/delta-<part-id>"
	echo '    result-folder       = <prefix>/output/<graph-name>'
	echo '  <graph-name>: <head>-<#-of-parts>, number of parts is automatically parsed from the last part of graph-name'
	echo '  <k-start> and <k-end>: the ID for delta-graphs (close range). Control the number of delta-graphs for each parameters. Can be used for parallelization'
	echo "  <log-sub-fdr>: sub folder for the log files. Log-folder = ../log/pr-$DELTA_NAME/<log-sub-fdr>"
	echo '  [degree]: (=0) use degree in setting priority'
	echo '  [snapshot]: (=0.1) the interval of termination check, in seconds'
	exit
fi

PRE=$1

FOLDER=$2
#FOLDER=tw6-1-1
#HEAD=$(echo $FOLDER | sed -r 's/-[0-9]+//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
#PARTS=1
#WORKERS=$(( PARTS+1 ))

K_START=$3
K_END=$4

GRAPH_FDR=$PRE/input/$FOLDER
RESULT_FDR=$PRE/output/$FOLDER
INIT_FDR=$PRE/ref/$FOLDER
DELTA_FDR=$PRE/delta/$FOLDER/$DELTA_NAME/

LOG_SUB_FDR=$5
LOG_FDR=../log/pr-$DELTA_NAME/$LOG_SUB_FDR

DEGREE=0
if [ $# -ge 6 ]; then
	DEGREE=$6
fi

SNAPSHOT=0.1
if [ $# -ge 7 ]; then
	SNAPSHOT=$7
fi

mkdir -p $RESULT_FDR
mkdir -p $DELTA_FDR
mkdir -p $LOG_FDR

DELTA_RATIOS="0.05"
CRT_RATIOS="0.2"
GOOD_RATIOS="0.2 0.8"
EW_RATIOS="0.2"

PORTIONS="1 0.1 0.01 0.001 0.0001 0.00001 0.000001"
ALPHAS="1"

N=3

./test-kernel-uw.sh Pagerank $PARTS $GRAPH_FDR $INIT_FDR $DELTA_FDR $RESULT_FDR $LOG_FDR $SNAPSHOT\
  "$DELTA_RATIOS" "$CRT_RATIOS" "$GOOD_RATIOS"  $K_START $K_END "$PORTIONS" "$ALPHAS" $DEGREE $N
