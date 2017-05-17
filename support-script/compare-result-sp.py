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

def loadResult(fn):
    with open(fn) as f:
        data=f.read().split('\n')
    pat=re.compile(r'(\d+)\t.+:(.+)')
    res=[]
    for line in data:
        m=pat.match(line)
        if m:
            res.append((int(m.group(1)), float(m.group(2))))
    res.sort()
    return res

def compareOne(r1, r2, show_detail):
    l1=len(r1)
    l2=len(r2)
    res=0
    cnt=0
    if l1!=l2:
        print('Error: number of nodes does match (',l1,'vs',l2,')')
        exit(1)
    for i in range(l1):
        if r1[i][0] != r2[i][0]:
            print('  keys do not match:', r1[i][0], r2[i][0])
            exit(1)
        diff=r1[i][1] - r2[i][1]
        if diff!=0:
            if show_detail:
                print('  diff on',r1[i][0],'diff:',diff)
            res+=abs(diff)
            cnt+=1
    return (res, cnt)

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
        print('Error: number of parts does not match. (',len(files1),'vs',len(files2),') Please try to enable option: merge_parts')
        exit(1)
    total=0
    cnt=0
    nKeys=0
    if merge_parts:
        r1=[]
        r2=[]
        for i in range(len(files1)):
            print('loading result 1 part',i)
            r1.extend(loadResult(path1+'/'+files1[i]))
        for i in range(len(files2)):
            print('loading result 2 part',i)
            r2.extend(loadResult(path2+'/'+files2[i]))
        r1.sort()
        r2.sort()
        #print(r1)
        #print(r2)
        (total, cnt) =compareOne(r1, r2, show_detail)
        nKeys=len(r1)
    else:
        for i in range(len(files1)):
            print('comparing part',i)
            r1=loadResult(path1+'/'+files1[i])
            r2=loadResult(path2+'/'+files2[i])
            (diff,num)=compareOne(r1, r2, show_detail)
            print('  # of different nodes:', num, '/', len(r1),', sub-total difference:',diff)
            total+=diff
            cnt+=num
            nKeys+=len(r1)
    print('Total different nodes', cnt, '/', nKeys)
    print('Total difference', total)

if __name__=='__main__':
    if len(sys.argv) < 3:
        print('Compare the results of two runs.')
        print('Usage: <result-path-1> <result-path-1> [show-detail] [merge-parts]')
        print('  [show-detail]: (=1) Show every found difference')
        print('  [merge-pars]: (=0) Merge the graph parts before comparison, in order to work with those cases using different number of workers.')
        exit()
    path1=sys.argv[1]
    path2=sys.argv[2]
    show_detail=True
    if len(sys.argv) > 3 and sys.argv[3] not in ['1', 'y', 'yes', 't', 'true']:
        show_detail=False
    merge_parts=False
    if len(sys.argv) > 4 and sys.argv[4] in ['1', 'y', 'yes', 't', 'true']:
        merge_parts=True
    #print('Merge parts before comparison =', merge_parts)
    main(path1, path2, merge_parts, show_detail)

