import sys, os, re

def get_file_names(folder):
    l=os.listdir(folder);
    rpat=re.compile(r'^band-\d+$')
    rfiles=[]
    for fn in l:
        if rpat.match(fn):
            rfiles.append(fn)
    rfiles.sort()
    return rfiles

def load_one_list(fn):
    fin=open(fn)
    res=[]
    d=fin.read()
    d=re.split(r'\s+', d)
    for v in d:
        if len(v)!=0:
            res.append(int(v))
    return res

# return the meraged result
def gather(folder, do_average, trans_bit):
    files=get_file_names(folder)
    data=[]
    n=0
    for f in files:
        x=load_one_list(folder+'/'+f)
        data.append(x)
        n=max(n,len(x))
    res=[0 for i in range(n)]
    for d in data:
        for j in range(n):
            res[j]+=d[j]
    if do_average:
        for i in range(n):
            res[i]/=len(files);
    if trans_bit:
        for i in range(n):
            res[i]*=8
    return res
    
def dump_result(output, res):
    if len(output)==0:
        for v in res:
            print(str(v)+' ', end='')
        print('')
    else:
        f=open(output,'w')
        for v in res:
            f.write(str(v)+' ')
        f.close()

def main(folder, output, do_average, trans_bit):
    res = gather(folder, do_average, trans_bit)
    dump_result(output, res)

if __name__=='__main__':
    if len(sys.argv) <= 1:
        print('Merge the bandwidth usage records.')
        print('Usage: <bandwidth-folder> [result-name] [do-average] [trans-to-bit]')
        print('  <bandwidth-folder>: The folder storing the bandwidth usage records. Their names should start with "band-"')
        print('  [result-name]: (="") The output file. If not give, the output will be put to console. (you may want to use > to redirect)')
        print('  [do-average]: (=0) Output the averaged bandwidth usage for all workers instead of their sum')
        print('  [trans-to-bit]: (=0) Translate the Byte data to Bit (*8 to each value)')
        exit()
    argc=len(sys.argv)
    folder=sys.argv[1]
    output=sys.argv[2] if argc > 2 else ""
    do_average=sys.argv[3] in ['1', 'y', 'yes', 't', 'true'] if argc > 3 else False
    trans_bit=sys.argv[4] in ['1', 'y', 'yes', 't', 'true'] if argc > 4 else False
    
    main(folder, output, do_average, trans_bit)


