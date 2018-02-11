if [ $# -ne 1 ]; then
	exit 1
fi

LOCAL_AGG_APPLICATION="Pagerank Adsorption Katz MarkovChain Jacobi"
DIR=$(dirname $0)
$DIR/contains.sh "$LOCAL_AGG_APPLICATION" $1
