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
            res.append(float(v))
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
        for j in range(len(d)):
            res[j]+=d[j]
    if do_average:
        for i in range(n):
            res[i]/=len(files);
    if trans_bit:
        for i in range(n):
            res[i]*=8
    return res
    
def dump_result(output, trans_unit, res):
    f=sys.stdout
    if len(output)!=0:
        f=open(output,'w')
    if trans_unit==1:
        for v in res:
            print(str(v)+'\t', end='', file=f)
    else:
        for v in res:
            print('%.2f\t' % (v/trans_unit), end='', file=f)
    print('', file=f)
    if len(output)!=0:
        f.close()

def main(folder, output, do_average, trans_unit, trans_bit):
    res = gather(folder, do_average, trans_bit)
    dump_result(output, trans_unit, res)

if __name__=='__main__':
    argc=len(sys.argv)
    if argc <= 1:
        print('Merge the bandwidth usage records.')
        print('Usage: <bandwidth-folder> [result-name] [do-average] [trans-to-kmg] [trans-to-bit]')
        print('  <bandwidth-folder>: The folder storing the bandwidth usage records. Their names should start with "band-"')
        print('  [result-name]: (="") The output file. If not give, the output will be put to console. (you may want to use > to redirect)')
        print('  [do-average]: (=0) Output the averaged bandwidth usage for all workers instead of their sum')
        print('  [trans-to-kmg]: (="") Translate teh value to Kilo (k), Million (m) or Giga (g). Accept: "", 1, k, m, g')
        print('  [trans-to-bit]: (=0) Translate the Byte data to Bit (*8 to each value)')
        exit()
    folder=sys.argv[1]
    output=sys.argv[2] if argc > 2 else ""
    do_average=sys.argv[3] in ['1', 'y', 'yes', 't', 'true'] if argc > 3 else False
    trans_unit=sys.argv[4].lower() if argc > 4 else '1'
    trans_bit=sys.argv[5] in ['1', 'y', 'yes', 't', 'true'] if argc > 5 else False
    
    if trans_unit in ['1', ' ', '']: trans_unit=1;
    elif trans_unit=='k': trans_unit=1000;
    elif trans_unit=='m': trans_unit=1000*1000;
    elif trans_unit=='g': trans_unit=1000*1000*1000;
    else:
        print('ERROR: unsupported trans-to-kmg option')
        exit()

    main(folder, output, do_average, trans_unit, trans_bit)


