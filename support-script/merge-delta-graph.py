import os, sys, re

def get_graph_file_names(graph_folder):
    l=os.listdir(graph_folder);
    gpat=re.compile(r'^part\d+$')
    gfiles=[]
    for fn in l:
        if gpat.match(fn):
            gfiles.append(fn)
    gfiles.sort()
    return gfiles

def get_delta_file_names(delta_prefix):
    folder=re.sub(r'[/\\]+[^/\\]+$','',delta_prefix)
    namep=re.sub(r'^.*[/\\]','',delta_prefix)
    l=os.listdir(folder);
    dpat=re.compile('^'+namep+r'-\d+$')
    dfiles=[]
    for fn in l:
        if dpat.match(fn):
            dfiles.append(fn)
    dfiles.sort()
    #dfiles=[namep+str(i) for i in range(n)]
    return (folder, dfiles)

def merge_weight(gfn, dfn):
    with open(gfn) as f:
        gdata=[l for l in f.read().split('\n') if len(l)!=0]
    with open(dfn) as f:
        ddata=[l for l in f.read().split('\n') if len(l)!=0]
    # parse graph
    g={}
    for i in range(len(gdata)):
        key, line=gdata[i].split('\t')
        line = [l.split(',') for l in line.split(' ') if len(l)!=0]
        g[int(key)]=[(int(e), float(w)) for e,w in line]
    # parse delta and apply
    cntA=0
    cntR=0
    cntI=0
    cntD=0
    for line in ddata:
        #print(line)
        tp = line[0]
        line=line[2:].split(',')
        f=int(line[0])
        t=int(line[1])
        w=float(line[2])
        if tp == 'A':
            g[f].append((t,w))
            cntA+=1
        elif tp == 'R':
            idx=[i for i in range(len(g[f])) if g[f][i][0]==t][0]
            del g[f][idx]
            cntR+=1
        elif tp == 'I':
            idx=[i for i in range(len(g[f])) if g[f][i][0]==t][0]
            g[f][idx]=(t,w)
            cntI+=1
        else:
            idx=[i for i in range(len(g[f])) if g[f][i][0]==t][0]
            g[f][idx]=(t,w)
            cntD+=1
    print('  added change:',cntA)
    print('  removed change:',cntR)
    print('  increase change:',cntI)
    print('  decrease change:',cntD)
    return g

# g is dict{key, list(<to, weight>)}
def dump_weight(fn, g):
    with open(fn, 'w') as f:
        for k, line in g.items():
            f.write(str(k))
            f.write('\t')
            for e, w in line:
                f.write(str(e))
                f.write(',')
                f.write(str(w))
                f.write(' ')
            f.write('\n')

def merge_unweight(gfn, dfn):
    with open(gfn) as f:
        gdata=[l for l in f.read().split('\n') if len(l)!=0]
    with open(dfn) as f:
        ddata=[l for l in f.read().split('\n') if len(l)!=0]
    # parse graph
    g={}
    for i in range(len(gdata)):
        key, line=gdata[i].split('\t')
        line = [l for l in line.split(' ') if len(l)!=0]
        g[int(key)]=[int(e) for e in line]
    # parse delta and apply
    cntA=0
    cntR=0
    for line in ddata:
        tp = line[0]
        line=line[2:].split(',')
        f=int(line[0])
        t=int(line[1])
        #w=float(line[2])
        if tp == 'A':
            g[f].append(t)
            cntA+=1
        elif tp == 'R':
            idx=[i for i in range(len(g[f])) if g[f][i]==t][0]
            del g[f][idx]
            cntR+=1
        else:
            pass
            #idx=[i for i in range(len(g[f])) if g[f][i]==t][0]
            #g[f][idx][1]=w
    print('  added change:',cntA)
    print('  removed change:',cntR)
    return g
    
# g is dict{key, list(to)}
def dump_unweight(fn, g):
    with open(fn, 'w') as f:
        for k, line in g.items():
            f.write(str(k))
            f.write('\t')
            for e in line:
                f.write(str(e))
                f.write(' ')
            f.write('\n')

def main(graph_folder, delta_prefix, output_folder, weighted):
    gfiles=get_graph_file_names(graph_folder)
    delta_folder, dfiles = get_delta_file_names(delta_prefix)
    print(gfiles)
    print(dfiles)
    if len(gfiles)==0:
        print('Error: cannot find graph files')
        exit(1)
    elif len(dfiles)==0:
        print('Error: cannot find delta files')
        exit(1)
    elif len(gfiles) != len(dfiles):
        print('Error: number of parts do not match with number of delta')
        exit(1)
    if weighted:
        fun_merge=merge_weight
        fun_dump=dump_weight
    else:
        fun_merge=merge_unweight
        fun_dump=dump_unweight
        
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for i in range(len(gfiles)):
        print('processing file',i)
        g=fun_merge(graph_folder+'/'+gfiles[i], delta_folder+'/'+dfiles[i])
        print('dumping file',i)
        fun_dump(output_folder+'/part'+str(i), g)

if __name__=='__main__':
    if len(sys.argv) < 4:
        print('Merge a graph with its delta-graph.')
        print('Usage: <grpah-path> <delta-prefix> <output-folder> [weighted]\n'
            '  [weighted]: (=1) whether the graph is weighted graph.')
        exit()
    graph_folder=sys.argv[1]
    delta_prefix=sys.argv[2]
    output_folder=sys.argv[3]
    weighted=True
    if len(sys.argv) > 4 and sys.argv[4].lower() not in {'1', 'y', 'yes', 't', 'true'}:
        weighted=False
    print('weighted graph = '+str(weighted))
    main(graph_folder, delta_prefix, output_folder, weighted)

