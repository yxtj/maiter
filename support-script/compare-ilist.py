import os, sys, re

def get_file_names(graph_folder):
    l=os.listdir(graph_folder);
    rpat=re.compile(r'^ilist-\d+$')
    rfiles=[]
    for fn in l:
        if rpat.match(fn):
            rfiles.append(fn)
    rfiles.sort()
    return rfiles

def loadInList(fn):
    with open(fn) as f:
        gdata=[line for line in f.read().split('\n') if len(line)!=0]
    g={}
    for i in range(len(gdata)):
        key, line=gdata[i].split('\t')
        line = [l.split(',') for l in line.split(' ') if len(l)!=0]
        #g[int(key)]=[(int(e), float(w)) for e,w in line]
        g[int(key)]=sorted([int(e) for e,w in line])
    return list(g.items())

def compareOne(g1, g2, show_detail):
    l1=len(g1)
    l2=len(g2)
    cnt=0
    if l1!=l2:
        print('Error: number of nodes does match')
        exit(1)
    for i in range(l1):
        if g1[i][0] != g2[i][0]:
            print('  keys do not match:', g1[i][0], g2[i][0])
            exit(1)
        if g1[i][1] != g2[i][1]:
            if show_detail:
                print('  diff on',g1[i][0],':',g1[i][1], 'vs.', g2[i][1])
            cnt+=1
    return cnt

def main(path1, path2, merge_parts, show_detail):
    files1=get_file_names(path1)
    files2=get_file_names(path2)
    print(files1)
    print(files2)
    if len(files1)==0:
        print('Error: cannot find result files in folder 1')
        exit(1)
    elif len(files2)==0:
        print('Error: cannot find result files in folder 2')
        exit(1)
    elif not merge_parts and len(files1) != len(files2):
        print('Error: number of parts does not match. Please try to enable option: merge_parts')
        exit(1)
    cnt=0
    nKeys=0
    if merge_parts:
        g1=[]
        g2=[]
        for i in range(len(files1)):
            print('loading result 1 part',i)
            g1.extend(loadInList(path1+'/'+files1[i]))
        for i in range(len(files2)):
            print('loading result 2 part',i)
            g2.extend(loadInList(path2+'/'+files2[i]))
        g1.sort()
        g2.sort()
        cnt =compareOne(g1, g2, show_detail)
        nKeys=len(g1)
    else:
        for i in range(len(files1)):
            print('comparing part',i)
            g1=loadInList(path1+'/'+files1[i])
            g2=loadInList(path2+'/'+files2[i])
            num=compareOne(g1, g2, show_detail)
            print('  # of different nodes:', num, '/', len(g1))
            cnt+=num
            nKeys+=len(g1)
    print('Total different nodes', cnt, '/', nKeys)

if __name__=='__main__':
    if len(sys.argv) < 3:
        print('Compare the in neighbor list of two runs.')
        print('Usage: <result-path-1> <result-path-1> [show-detail] [merge-parts]')
        print('  [show-detail]: (=1) Show every found difference')
        print('  [merge-pars]: (=0) Merge the graph parts before comparison, in order to work with those cases using different number of workers.')
        exit()
    path1=sys.argv[1]
    path2=sys.argv[2]
    merge_parts=False
    if len(sys.argv) > 3 and sys.argv[3] in ['1', 'y', 'yes', 't', 'true']:
        merge_parts=True
    show_detail=True
    if len(sys.argv) > 4 and sys.argv[4] not in ['1', 'y', 'yes', 't', 'true']:
        show_detail=False
    #print('Merge parts before comparison =', merge_parts)
    main(path1, path2, merge_parts, show_detail)

