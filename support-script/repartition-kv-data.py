import os, sys, re

def get_file_names(graph_folder):
    l=os.listdir(graph_folder);
    rpat=re.compile(r'^part-\d+$')
    rfiles=[]
    for fn in l:
        if rpat.match(fn):
            rfiles.append(fn)
    rfiles.sort()
    return rfiles

# load data with '\n'
def load_lines(fn):
    with open(fn) as f:
        data=f.readlines()
    return data

def split_dump(data, fout, sepper):
    n=len(fout)
    for line in data:
        p=line.find('\t')
        if p == -1:
            continue
        k=int(line[:p])
        s=k%n
        fout[s].write(line)


def main(in_path, out_dir, name_prefix, n_parts):
    print('loading original data')
    data=load_lines(in_path)
    if len(data)==0:
        print('Error: input file cannot be open or is empty.')
        exit()
    print('  lines loaded: ', len(data))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    fout=[open(out_dir+'/'+name_prefix+str(i), 'w') for i in range(n_parts)]
    print('spliting data...')
    split_dump(data, fout, '\t')
    for f in fout:
        f.close()

if __name__=='__main__':
    if len(sys.argv) <= 4:
        print('Split a key-value file into parts by the key.')
        print('Usage: <in-file-path> <out-dir> <out-name-prefix> <#parts>')
        print('  <in-file-path>: the path to the input file. The format of the input file is "<key>\\t<value>"')
        print('  <out-dir>: the directory of the output files.')
        print('  <out-name-prefix>: the prefix of the output files. The full name is followed with an part-id: <out-name-prefix><part-id>.')
        print('  <#parts>: the number of parts to split the input file.')
        exit()
    in_path=sys.argv[1]
    out_dir=sys.argv[2]
    name_prefix=sys.argv[3]
    n_parts=int(sys.argv[4])
    main(in_path, out_dir, name_prefix, n_parts)

