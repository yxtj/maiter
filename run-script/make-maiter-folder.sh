if [ $# -lt 2]; then
  echo 'Usage: <prefix> <name>'
fi

prefix=$1
name=$2
for f in input output delta ref; do mkdir -p $prefix/$f/$name; done
