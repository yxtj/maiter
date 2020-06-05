# get logs

#if [  ]

FOLDER=$1
ESCAPED_FOLDER=$(echo $FOLDER/ | sed -e 's/[]\/$*.^[]/\\&/g')

for fn in $(ls $FOLDER/t*); do echo $fn $(grep "Starting new checkpoint" $fn | wc -l); done | sed "s/$ESCAPED_FOLDER//g"
#grep "Starting new checkpoint" $FOLDER/* | wc -l > smy_count
