if [ $# -lt 2 ]; then
	echo 'Usage: <folder> <delta-name> [delta-ratio] [add-r] [rmv-r] [inc-r] [dec-r] [seed]'
	echo '  [delta-ratio]: (=0.05)'
	echo '  [add-r]: (=0.2)'
	echo '  [rmv-r]: (=0.2)'
	echo '  [inc-r]: (=0.3)'
	echo '  [dec-r]: (=0.3)'
	echo '  [seed]: (=123456)'
	exit
fi

FOLDER=$1
HEAD=$(echo $FOLDER|sed -r 's/-[0-9]+//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
DELTA_NAME=$2

DELTA_RATIO=0.05
if [ $# -ge 3 ]; then
	DELTA_RATIO=$3
fi
ADD_R=0.2
if [ $# -ge 4 ]; then
	ADD_R=$4
fi
RMV_R=0.2
if [ $# -ge 5 ]; then
	RMV_R=$5
fi
INC_R=0.3
if [ $# -ge 6 ]; then
	INC_R=$6
fi
DEC_R=0.3
if [ $# -ge 7 ]; then
	DEC_R=$7
fi
SEED=123456
if [ $# -ge 8 ]; then
	SEED=$8
fi

echo 'generating delta graph...'
../gen/deltaGen.exe ../test/input/$FOLDER/ $PARTS $DELTA_NAME $DELTA_RATIO $ADD_R $RMV_R $INC_R $DEC_R $SEED

MERGED_FOLDER=$HEAD-$DELTA_NAME-$PARTS

echo 'merging delta graph...'
python3 ../support-script/merge-delta-graph.py ../test/input/$FOLDER $DELTA_NAME ../test/input/$MERGED_FOLDER

WORKERS=$(( $PARTS + 1 ))

echo 'running benchmark...'
mkdir -p ../log/ref
mkdir -p ../test/ref/$MERGED_FOLDER
time -p { ./go-sp.sh $MERGED_FOLDER 2>../log/ref/$MERGED_FOLDER.txt ; }
rm -rf ../test/output/$MERGED_FOLDER
mv ../test/output/$MERGED_FOLDER ../test/ref/

echo 'running tested version...'
time -p { ./go-sp.sh $FOLDER ../test/ref/$FOLDER $DELTA_NAME 2>../log/$MERGED_FOLDER.txt ; }

echo 'comparing result...'
python3 ../support-script/compare-result.py ../test/ref/$MERGED_FOLDER ../test/output/$MERGED_FOLDER


