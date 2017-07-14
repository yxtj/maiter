import sys,os,re
from mpi4py import MPI
import shutil

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def list_files(src, prefix):
	pattern=re.compile(prefix+r'\d+')
	l=[fn for fn in os.listdir(src) if os.path.isfile(src+fn) and pattern.match(fn)]
	l.sort()
	return l

def main(src, dst, prefix, offset):
	if src[-1] not in ['/','\\']:
		src=src+'/'
	if dst[-1] not in ['/','\\']:
		dst=dst+'/'
	l=list_files(src, prefix)
	idx=rank-offset
	pat_name=re.compile(prefix+r'0*'+str(idx)+r'$')
	name=None
	for fn in l:
		if pat_name.match(fn):
			name=fn
			break
	if not os.path.exists(dst):
		os.makedirs(dst)
	if name:
		print('  rank',rank,'found file',name)
		shutil.copy2(src+name, dst+name)
	else:
		print('  rank',rank,'cannot found a related file')
	

if __name__=='__main__':
	argc=len(sys.argv)
	if argc != 4+1:
		if rank != 0:
			exit(1)
		print('Distribute files onto each MPI instance. Copy from NFS to a local location.')
		print('Usage: mpiexec -n XX -hostfile XX python3 distribute-mpi-files.py <src-dir> <dst-dir> <file-prefix> <offset>')
		print('  <src-dir>: the source folder (a shared location like NFS)')
		print('  <dst-dir>: the destination folder (a local location)')
		print('  <file-prefix>: the files to distribute (like: part, cedge-)')
		print('  <offset>: the first MPI rank to copy the files (the first instance may be a master without data file requirement)')
		print('eg. ... distribute-mpi-files /shared/graph /local/cache part 1')
		exit(1)
	src=sys.argv[1]
	dst=sys.argv[2]
	prefix=sys.argv[3]
	offset=int(sys.argv[4])
	if not os.path.isdir(src):
		if rank == 0:
			print('Error: cannot open source folder')
		exit(1)
	if rank == 0:
		print('Run with',size,'mpi instances. Distribute files from 0 to',size-offset)
	if rank < offset:
		exit(0)
	main(src, dst, prefix, offset)
	
