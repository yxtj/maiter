import re, sys, os

NUMBER_PAT=r'[0-9]*\.?[0-9]+'
NAME_PAT=r'(.+)_(.+)_(\d+)_('+NUMBER_PAT+r')-(' +NUMBER_PAT+ r')'

K_PART_PAT=r'--> total_time: ('+NUMBER_PAT+r') shard_time: ('+NUMBER_PAT+r') calls: (\d+) shard_calls: (\d+)'
K_LOAD_PAT=r'MaiterKernel1:run'
K_DELTA_PAT=r'MaiterKernelLoadDeltaGraph:run'
K_CMP_PAT=r'MaiterKernel2:map'
K_DUMP_PAT=r'MaiterKernel3:run'

K_PAT=re.compile((r'(%s|%s|%s|%s)'% (K_LOAD_PAT, K_DELTA_PAT, K_CMP_PAT, K_DUMP_PAT))+K_PART_PAT )

def map_kernel(kname):
    if kname == K_LOAD_PAT:
        return 0
    elif kname == K_DELTA_PAT:
        return 1
    elif kname == K_CMP_PAT:
        return 2
    elif kname == K_DUMP_PAT:
        return 3
    else:
        return -1

def get_key_from_name(fn):
    # graph, delta, k, top, alpha
    m = re.match(NAME_PAT, fn)
    if m:
        return (m.group(1), m.group(2), int(m.group(3)), m.group(4), m.group(5))
    return None

def get_time_one_file(fn):
    with open(fn) as f:
        data=f.read()
        l=K_PAT.findall(data)
        res=[0 for i in range(4)]
        for m in l:
            idx=map_kernel(m[0])
            total_t='%.3f' % float(m[1])
            #shard_t=m[2]
            res[idx]=total_t
        return res if all(isinstance(v, str) for v in res) else None
    return None

def get_statistics(folder):
    if folder[-1] not in ['/', '\\']:
        folder += '/'
    l = os.listdir(folder)
    res={}
    for fn in l:
        if os.path.isfile(folder+fn):
            key=get_key_from_name(fn)
            if key is None:
                print('Error on file name:', key)
                continue
            value=get_time_one_file(folder+fn)
            if value is None:
                print('Error in file content:', key)
                continue
            res[key]=value
    return res

def main(folder, out_file, append):
    print('loading statistics', file=sys.stderr)
    res=get_statistics(folder)
    print('  load %d files' % len(res), file=sys.stderr)

    mode = 'a' if append else 'w'
    put_header = mode == 'w' or not os.path.exists(out_file)
    try:
        f=open(out_file, mode)
        if put_header:
            f.write('#graph	delta	k	top	alpha	ld-g	ld-d	comp	dump-res')
            f.write('\n')
        for key, value in sorted(res.items()):
            line1='\t'.join(str(k) for k in key)
            line2='\t'.join(value)
            line=line1+'\t'+line2
            f.write(line)
            f.write('\n')
        f.close()
    except IOError:
        print("Error: cannot open output file")

if __name__=='__main__':
    argc=len(sys.argv)
    if argc <= 2:
        print('Get the time statistics from a set of log files')
        print('Usage: <log-folder> <out-file> [append]')
        print('  [append]: (=0) whether to append the out-file or create a new one. '
            'If the given file does not exist, output a header line.')
        exit()
    folder = sys.argv[1]
    out_file = sys.argv[2]
    append = False
    if argc > 3 and sys.argv[3].lower() in ['1', 'y', 't', 'yes', 'true']:
        append = True
    main(folder, out_file, append)

