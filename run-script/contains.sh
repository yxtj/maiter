if [ $# -ne 2 ]; then
	exit 1
fi

list=$1
item=$2
[[ $list =~ (^|[[:space:]])$item($|[[:space:]]) ]] && echo 1 || echo 0
