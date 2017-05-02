import math,sys,os,re

def loadData(fn):
	f=open(fn);
	res={};
	for line in f:
		t=line.split('\t')
		key=int(t[0])
		t=t[1].split(':')
		delta=float(t[0]);value=float(t[1]);
		res[key]=(delta,value)
	return res

def listFiles(folder):
	res=[]
	for fn in os.listdir(folder):
		if(re.match('part-\d+',fn)):
			res.append(folder+'/'+fn);
	return res

def loadDataFolder(folder):
	flist=listFiles(folder);
	res={}
	for fn in flist:
		res.update(loadData(fn));
	return res

def update_stat(stat,t):
	#min,max,sum,sqsum
	stat[0]=min(t,stat[0])
	stat[1]=max(t,stat[1])
	stat[2]+=t;
	stat[3]+=t**2;

def cmp_err(bench,out):
	#min,max,mean,std.d.
	inf=float('inf')
	st_d=[inf,-inf,0.0,0.0];
	st_v=[inf,-inf,0.0,0.0];
	for (k,v) in out.items():
		vb=bench[k];
		dif_d=(v[0]-vb[0])
		dif_v=(v[1]-vb[1])
		update_stat(st_d,dif_d)
		update_stat(st_v,dif_v)
	n=len(bench)
	st_d[2]/=n;	st_d[3]=math.sqrt(st_d[3]/n)
	st_v[2]/=n;	st_v[3]=math.sqrt(st_v[3]/n)
	return (st_d,st_v)

def print_cmp(stat):
	print ' min:',stat[0]
	print ' max:',stat[1]
	print ' mean:',stat[2]
	print ' std:',stat[3]

def main():
	print 'bench dir=',bench_dir;
	print 'output dir=',output_dir;
	ben=loadDataFolder(bench_dir);
	out=loadDataFolder(output_dir);
#	print ben
#	print out
	if ben.keys()!=out.keys():
		print 'ERROR: contain different set of keys.'
		print 'add: ',set(out.keys())-set(ben.keys())
		print 'delete: ',set(ben.keys())-set(out.keys())
		return;
	(st_d,st_v)=cmp_err(ben,out)
	print 'delta:'
	print_cmp(st_d)
	print 'value:'
	print_cmp(st_v)

if __name__=='__main__':
	bench_dir='output-bench/'
	output_dir='output/'
	if len(sys.argv)>1:
		bench_dir+=sys.argv[1];
	else:
		print 'usage: <bench-folder> [<output-folder>]'
	if len(sys.argv)>2:
		output_dir+=sys.argv[2];
	else:
		output_dir+='tp'
		print 'using default output-folder (tp).'
	main(bench_dir, output_dir)



