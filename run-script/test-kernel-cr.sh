# used to enumerate among all 7 parameters: 
# delta-ratio, cri-ratio, good-ratio, ew-ratio, 
# k-range (close range between k-start and k-end)
# top-portion, priority-alpha

if [ $# -lt 17 ] || [ $# -gt 18 ] ; then
	echo "Require 17~18 parameters but $# is given. Try to use \" \" arround ratios"
	echo "  1: algorithm, 2: #-parts, "
	echo "  3: graph-folder, 4: initial-folder, 5: delta-folder, 6: result-folder, 7: log-folder, "
	echo "  8: snapshot-interval, "
	echo "  9: delta-ratios, 10: critical-edge-ratios, 11: good-ratios, 12: edge-weight-ratios, "
	echo "  13: k-start, 14: k-end, "
	echo "  15: top-portions, 16: alphas, 17: priority-degree (1/0), "
	echo "  18: n-parallel (=1)"
	exit
fi

# basic parameters
ALGORITHM=$1
PARTS=$2
GRAPH_FDR=$3
INIT_FDR=$4
CE_FDR=$INIT_FDR
DELTA_FDR=$5
RESULT_FDR=$6
LOG_FDR=$7

SNAPSHOT=$8

FOLDER=$(basename $GRAPH_FDR)

# delta parameters
DELTA_RATIOS=$9
CRT_RATIOS=${10}
GOOD_RATIOS=${11}
EW_RATIOS=${12}

K_START=${13}
K_END=${14}

# run parameters
PORTIONS=${15}
ALPHAS=${16}
DEGREE=${17}

N=1
if [ $# -ge 18 ] && [ ! -z ${18// /} ]; then
	N=${18}
fi
#echo $@
for dr in $DELTA_RATIOS; do for cr in $CRT_RATIOS; do
	drn=$( printf '%.0f\n' $(echo "100*$dr"|bc))
	crn=$( printf '%.0f\n' $(echo "10*$cr"|bc))
	for gr in $GOOD_RATIOS; do for ew in $EW_RATIOS; do
		grn=$( printf '%.0f\n' $(echo "10*$gr"|bc))
		ewn=$( printf '%.0f\n' $(echo "10*$ew"|bc))
		dname=c$drn-$crn-$grn-$ewn
		for k in $(seq $K_START $K_END); do
			temp_result_fdr=$RESULT_FDR/$k/$dname
			temp_delta_fdr=$DELTA_FDR/$k/$dname	# use temporary folders for output (one for each k, inorder to parallelize)
			#echo $temp_result_fdr
			#echo $temp_delta_fdr
			mkdir -p $temp_result_fdr
			mkdir -p $temp_delta_fdr
			delta_pre=$temp_delta_fdr/delta
			log_name_prefix=$(printf '%s_%s_%d_' $FOLDER $dname $k)
			
			echo "processing delta graph: $k/$dname"
			if [ ! -f $temp_delta_fdr/delta-0 ]; then
#				delta_4_ratios=$(./cal_delta_gen_ratio.sh $gr $ew)
#				gen/delta-gen.exe $PARTS $GRAPH_FDR $delta_pre $dr $delta_4_ratios 1 $k > /dev/null
				../gen/delta-gen-ce.exe $PARTS $GRAPH_FDR $CE_FDR $delta_pre $dr $cr $gr $ew 1 $k > /dev/null
			fi
#			break
			i=1
			for po in $PORTIONS; do for al in $ALPHAS; do
				echo "  calculating p=$po a=$al"
				#echo 'Usage: <algorithm> <#-parts> <graph-fdr> <init-fdr> <delta-prefix> <result-fdr> [portion] [alpha] [snapshot] [verbose] [hostfile]'
				./go-delta.sh $ALGORITHM $PARTS $GRAPH_FDR $INIT_FDR $delta_pre $temp_result_fdr $po $al $DEGREE $SNAPSHOT 0 2>$LOG_FDR/$log_name_prefix$po-$al &
				((i=i%N)); ((i++==0)) && wait
			done; done
			wait
#			rm -rf $temp_delta_fdr
			rm -rf $temp_result_fdr
		done
	done; done
done; done
