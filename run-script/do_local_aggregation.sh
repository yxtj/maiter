if [ $# -ne 1 ]; then
	exit 1
fi

LOCAL_AGG_APPLICATION="Pagerank Adsorption Katz"

./contains.sh "$LOCAL_AGG_APPLICATION" $1
