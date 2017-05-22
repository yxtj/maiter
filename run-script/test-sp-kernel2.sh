# used to enumerate among all 7 parameters: 
# delta-ratio, cri-ratio, good-ratio, ew-ratio, 
# k-range (close range between k-start and k-end)
# top-portion, priority-alpha

if [ $# -ne 14 ]; then
	echo "Require 14 parameters but $# is given. Try to use \" \" arround ratios"
	echo "  1: #-parts, "
	echo "  2: graph-folder, 3: initial-folder, 4: delta-folder, 5: result-folder, 6: log-folder, "
	echo "  7: snapshot-interval, "
	echo "  8: delta-ratios, 9: critical-edge-ratios, 10: good-ratios, 11: edge-weight-ratios"
	echo "  12: k-start, 13: k-end, "
	echo "  14: top-portions, 15: alphas"
	exit
fi

# basic parameters
PARTS=$1
GRAPH_FDR=$2
INIT_FDR=$3
CE_FDR=$INIT_FDR
DELTA_FDR=$4
RESULT_FDR=$5
LOG_FDR=$6

SNAPSHOT=$7

FOLDER=$(basename $GRAPH_FDR)

# delta parameters
DELTA_RATIOS=$8
CRT_RATIOS=$9
GOOD_RATIOS=${10}
EW_RATIOS=${11}

K_START=${12}
K_END=${13}

# run parameters
PORTIONS=${14}
ALPHAS=${15}

for dr in $DELTA_RATIOS; do for cr in $CRT_RATIOS; do
	drn=$( printf '%.0f\n' $(echo "100*$dr"|bc))
	crn=$( printf '%.0f\n' $(echo "10*$cr"|bc))
	for gr in $GOOD_RATIOS; do for ew in $EW_RATIOS; do
		grn=$( printf '%.0f\n' $(echo "10*$gr"|bc))
		ewn=$( printf '%.0f\n' $(echo "10*$ew"|bc))
		dname=c$drn-$crn-$grn-$ewn
		for k in $(seq $K_START $K_END); do
			temp_result_fdr=$RESULT_FDR/$k
			temp_delta_fdr=$DELTA_FDR/$k	# use temporary folders for output (one for each k, inorder to parallelize)
			#echo $temp_result_fdr
			#echo $temp_delta_fdr
			mkdir -p $temp_result_fdr
			mkdir -p $temp_delta_fdr
			delta_pre=$temp_delta_fdr/delta
			log_name_prefix=$(printf '%s_%s_%d_' $FOLDER $dname $k)
			
			echo "generating delta graph: $log_name_prefix"
#			delta_4_ratios=$(./cal_delta_gen_ratio.sh $gr $ew)
#			../gen/deltaGen.exe $GRAPH_FDR $PARTS $delta_pre $dr $delta_4_ratios $k > /dev/null
			../gen/delta-gen2.exe $PARTS $GRAPH_FDR $CE_FDR $delta_pre $dr $cr $gr $ew $k > /dev/null
#			break
			for po in $PORTIONS; do for al in $ALPHAS; do
				echo "  calculating p=$po a=$al"
				#echo 'Usage: <#-parts> <graph-fdr> <init-fdr> <delta-prefix> <result-fdr> [portion] [alpha] [snapshot] [verbose] [hostfile]'
				./go-sp-delta.sh $PARTS $GRAPH_FDR $INIT_FDR $delta_pre $temp_result_fdr $po $al $SNAPSHOT 0 2>$LOG_FDR/$log_name_prefix$po-$al
			done; done
			rm -rf $temp_delta_fdr
			rm -rf $temp_result_fdr
		done
	done; done
done

