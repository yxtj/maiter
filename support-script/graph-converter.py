import re,sys

def load_data(fn):
	with open(fn, 'r') as f:
		data=f.read()
	data=data.split('\n')
	data1=[]
	for line in data:
		if len(line)!=0:
			data1.append(line.split('\t'))
	del data
	pat=re.compile(r'(\d+),.+')
	res={}
	for k,temp in data1:
		temp=temp.split(' ')[:-1]
		xx=[int(pat.sub(r'\1',x)) for x in temp]
		res[int(k)]=xx
	return res

def trans_to_in_graph(data):
	res={}
	for k, neighs in data.items():
		for v in neighs:
			if v not in res:
				res[v]=[k]
			else:
				res[v].append(k)
	for k in res:
		res[k].sort()
	return res
	
def print_graph(data):
	ks=list(data.keys())
	ks.sort()
	for k in ks:
		print(str(k)+'\t'+(' '.join([str(v) for v in data[k]])))

def main(filename, i2o):
	data=load_data(filename)
	if i2o:
		data=trans_to_in_graph(data)
	print_graph(data)

if __name__=='__main__':
	if len(sys.argv) < 2:
		print('Convert the in-neighbor result file back to normal out-list graph file')
		print('Usage: <filename> [i2o]')
		print('  [i2o]: (=true) translate the in-neighbor graph to out-neighbor graph.')
		exit()
	fn=sys.argv[1]
	i2o=True
	if len(sys.argv) > 2:
		i2o = sys.argv[2].lower() in {'true', 't', '1', 'yes', 'y'}
	main(fn, i2o)
	
