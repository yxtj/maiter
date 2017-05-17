
FOLDER=$1
HEAD=$(echo $FOLDER|sed -r 's/-[0-9]+//')
PARTS=$(echo $FOLDER | sed -r 's/^.*?-//g')
#DELTA_NAME=$2

DELTA_RATIO=0.05
PORTION=1
if [ $# -ge 3 ]; then
	#DELTA_RATIO=$3
	PORTION=$3
fi
ADD_R=0.3
ALPHA=1
if [ $# -ge 4 ]; then
	#ADD_R=$4
	ALPHA=$4
fi
RMV_R=0.0
if [ $# -ge 5 ]; then
	RMV_R=$5
fi
INC_R=0.0
if [ $# -ge 6 ]; then
	INC_R=$6
fi
DEC_R=0.7
if [ $# -ge 7 ]; then
	DEC_R=$7
fi
SEED=123456
if [ $# -ge 8 ]; then
	SEED=$8
fi


PRE=/tmp/gz/tw6/tw6-1-
DELT=delta
for k in 1 2 4 7 9
do
	for dd in $( seq 1 3)
	do
		#FOLDER=$PRE$k

		for dr in 0.001 0.01 0.1
		do
			DELTA_NAME=$DELT$dd-$dr
			echo 'generating delta graph... graph#' $k 'instance' $dd 'deltaRate' $DELTA_NAME
			./deltaGen.exe $PRE$k 1 $DELTA_NAME $dr $ADD_R $RMV_R $INC_R $DEC_R $dd


			for pp in 0.005 0.01 0.05
			do
				PORTION=$pp
				for alp in 0 0.1 0.2 0.3 0.7 100
				do
					ALPHA=$alp
					MERGED_FOLDER=tw6-1-$k-$DELTA_NAME-po$PORTION-alp$ALPHA-good
					WORKERS=$(( $PARTS + 1 ))
					FOLDER=tw6-1-$k

				echo 'running tested version...' $MERGED_FOLDER
				{ time -p { ./go-sp2.sh $FOLDER ref/$FOLDER $DELTA_NAME $PORTION $ALPHA ; } ; } 2>> ../log/new/$MERGED_FOLDER.txt
			
				done
			done
		done
	done
done



	
#echo 'merging delta graph...'
#python3 ../support-script/merge-delta-graph.py ../test/input/$FOLDER $DELTA_NAME ../test/input/$MERGED_FOLDER


#echo 'running benchmark...'
#mkdir -p ../log/ref
#mkdir -p ../test/ref/$MERGED_FOLDER
#time -p { ./go-sp.sh $MERGED_FOLDER 2>../log/ref/$MERGED_FOLDER.txt ; }
#rm -rf ../test/output/$MERGED_FOLDER
#mvcd  ../test/output/$MERGED_FOLDER ../test/ref/


#echo 'comparing result...'
#python3 ../support-script/compare-result.py ../test/ref/$MERGED_FOLDER ../test/output/$MERGED_FOLDER


