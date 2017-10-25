if [ $# -lt 1 ]; then
	echo 'Merge the raw bandwidth usage records of multiple runs with different parameters.'
	echo '  Requires: log files are named in format: log-$p-$i . $p is the parameter, $i is the runinng id'
	echo '  Output: 1) bandwidth usage forder for each parameter (foldder $p-$i). (intermediate)'
	echo '          2) averged bandwidth usage file for each parameter (file band-$p). (final output)'
	echo 'Usage: <folder> [parameters] [ids]'
	echo '  By default [parameters] and [ids] try to use all the available files in given folder.'
	echo 'DO NOT use $(seq n) directly in giving parameters. Use LIST=$(seq n) and $LIST instead.'
	exit
fi

FOLDER=$1

if [ $# -ge 2 ]; then 
	PARAMS=$2
else
	PARAMS=$(basename -a $(ls $FOLDER/log-*-1) | sed 's/log-//g' | sed 's/-1//g')
fi

GIVEN_IDS=""
if [ $# -ge 3 ]; then 
	GIVEN_IDS=$3
fi

MYFOLDER=$(dirname $0)

echo Parameters: $PARAMS
echo Given_IDs: $GIVEN_IDS
#echo My folder: $MYFOLDER

for p in $PARAMS; do 
	echo $p
	if [ -z $GIVEN_IDS ]; then
		IDS=$(basename -a $(ls $FOLDER/log-$p-*) | sed "s/log-$p-//g")
	else
		IDS=GIVEN_IDS
	fi
#	echo $IDS
	mkdir -p $FOLDER/$p
	for i in $IDS; do
		python3 $MYFOLDER/merge-bandwidth-usage.py $FOLDER/$p-$i $FOLDER/$p/band-$i 0 m 1
	done
	python3 $MYFOLDER/merge-bandwidth-usage.py $FOLDER/$p $FOLDER/band-$p 1 1 0
done
