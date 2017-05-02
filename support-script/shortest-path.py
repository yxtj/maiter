import os, sys, re

def get_file_names(graph_folder):
    l=os.listdir(graph_folder);
    gpat=re.compile(r'^part\d+$')
    gfiles=[]
    for fn in l:
        if gpat.match(fn):
            gfiles.append(fn)
    gfiles.sort()
    return gfiles

def load_graph(fn):
	with open(fn, 'r') as f:
		data=[line for line in f.read() if len(len)!=0]
	data1=[]
	for line in data:
		if len(line)!=0:
			data1.append(line.split('\t'))
	del data
	pat=re.compile(r'(\d+),(.+)')
	res={}
	for k,temp in data1:
		temp=temp.split(' ')[:-1]
		l={}
		for edge in temp.split(' '):
			m=pat.match(edge)
			if not m:
				continue
			l[int(m.group(1))] = float(m.group(2))
		res[int(k)]=l
	return res

# SPFA algorithm
def shortest_path_SPFA(g, src):
	inf=float('inf')
	res={}
	for k in g:
		res[k]=inf
	res[src]=0
	queue=[]
	queue.extend(g[src].keys())
	

def dump_result(fn, res):
	with open(fn, 'w') as f:
		for k,v in res.items():
			fwrite(str(k))
			fwrite('\t')
			fwrite(str(v))
			fwrite(':')
			fwrite(str(v))

def main(graph_folder, output_folder, source):
    gfiles = get_file_names(graph_folder)
    print(gfiles)
    if len(gfiles)==0:
        print('Error: cannot find graph files')
        exit(1)
    g={}
    for i in range(len(gfiles)):
        print('  loading file',i)
        g.update(load_graph(graph_folder+'/'+gfiles[i]))
    res=shortest_path_SPFA(g, source)
    print('calculating shortest path')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    print('outputting')
    for i in range(len(gfiles)):
        dump_result(graph_folder+'/part-0', res)

if __name__=='__main__':
    if len(sys.argv) < 4:
        print('Calculate the shortest distance.')
        print('Usage: <grpah-path> <output-path> [source-id]')
        print('  [source-id]: (=0) the source node')
        exit()
    graph_folder=sys.argv[1]
    output_folder=sys.argv[2]
    source=0
    if len(sys.argv) > 3
        source=sys.argv[3]
    main(graph_folder, output_folder, source)

