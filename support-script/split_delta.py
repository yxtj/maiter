import os, sys, re

def get_file_names(graph_folder, delta_name):
    l=os.listdir(graph_folder)
    dpat=re.compile('^'+delta_name+r'-\d+$')
    dfiles=[]
    for fn in l:
        if dpat.match(fn):
            dfiles.append(fn)
    dfiles.sort()
    return dfiles

def load_delta(fn):
    d=[]
    with open(fn) as f:
        for line in f:
            if len(line)<=2:
                continue
            t=line[0]
            d.append((t, line))
    return d

def split_delta(ddata, type1, type2):
    d1=[]
    d2=[]
    for line in ddata:
        t = line[0]
        if t in type1:
            d1.append(line[1])
        if t in type2:
            d2.append(line[1])
    return d1, d2

def dump_delta(fn, ddata):
    with open(fn, 'w') as f:
        for line in ddata:
            f.write(line)
            f.write('\n')

def main(graph_folder, delta_name, AD_delta_name, RI_delta_name, weight):
    dfiles = get_file_names(graph_folder, delta_name)
    print(dfiles)
    if len(dfiles)==0:
        print('Error: cannot find delta files')
        exit(1)
    for i in range(len(dfiles)):
        print('  processing file',i)
        ddata=load_delta(graph_folder+'/'+dfiles[i])
        if len(ddata) == 0:
            print('  Warnning: empty input delta file')
        d1, d2 = split_delta(ddata, ['A', 'D'], ['R', 'I'])
        print('  total:', len(ddata),', type1:',len(d1),', type2:',len(d2))
        dump_delta(graph_folder+'/'+AD_delta_name+'-'+str(i), d1)
        dump_delta(graph_folder+'/'+RI_delta_name+'-'+str(i), d2)

if __name__=='__main__':
    if len(sys.argv) < 4:
        print('Merge a graph with its delta-graph.')
        print('Usage: <grpah-path> <delta-name> <AD-delta-name> <RI-delta-name>')
        exit()
    graph_folder=sys.argv[1]
    delta_name=sys.argv[2]
    AD_delta_name=sys.argv[3]
    RI_delta_name=sys.argv[4]
    weight=True
    if len(sys.argv) > 5 and sys.argv[5].lower() not in {'1', 'y', 'yes', 't', 'true'}:
        weight=False
    print('weighted graph = '+str(weight))
    main(graph_folder, delta_name, AD_delta_name, RI_delta_name, weight)

