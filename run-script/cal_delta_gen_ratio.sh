#! /bin/sh

if [ $# -lt 2 ]; then
	return 1
fi

#local gr=$1
gr=$1
#local ew=$2
ew=$2
add_r=$(echo " $ew*$gr" | bc -l)
rmv_r=$(echo "$ew*(1-$gr)" | bc -l)
inc_r=$(echo "(1 - $ew)*(1-$gr)" | bc -l)
dec_r=$(echo "(1-$ew)*$gr" | bc -l)
echo "$add_r $rmv_r $inc_r $dec_r"

return 0
