# get logs

#if [  ]

FOLDER=$1
ESCAPED_FOLDER=$(echo $FOLDER/ | sed -e 's/[]\/$*.^[]/\\&/g')

#grep time t5-6-0.001-* |grep Kernel2 | cut -d " " -f 1,7 --output-delimiter='-' | sed -E "s/:I[0-9]+//" | cut -d "-" -f 1- --output-delimiter=' '
#grep time $FOLDER/* |grep Kernel2 | sed -E "s/ +/ /g" | cut -d " " -f 1,7 --output-delimiter='-' | sed -E "s/:I[0-9]+//" | cut -d "-" -f 1- --output-delimiter=' '
grep time $FOLDER/t* |grep Kernel2 | sed -E "s/ +/ /g" | sed "s/$ESCAPED_FOLDER//g" | cut -d " " -f 1,7 --output-delimiter='-' | sed -E "s/:I[0-9]+//"
# output format: graph_size workers portion term-intv cp_intv cp_type time

