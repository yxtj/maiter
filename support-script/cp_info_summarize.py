# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 10:25:41 2020

@author: Tian
"""

import re
import numpy as np
import sys

#data = pandas.read_csv('../data/maiter-pagerank.txt','\t',header=None)

def load_time_file(fn):
    data = None
    with open(fn) as f:
        data = f.readlines()
    if data is None:
        return None
    data = [line[:-1].split(' ') for line in data]
    return data


def load_cp_count_file(fn):
    data = None
    with open(fn) as f:
        data = f.readlines()
    if data is None:
        return None
    data = [line[:-1].split(' ') for line in data]
    return data


class Item():
    def __init__(self, name):
        m = re.match('''t(\d+)-(\d+)-(\d+\.\d+)-(\d+(?:\.\d*)?)-(\d+(?:\.\d*)?)-(\w+)''', name)
        #print(m.groups())
        self.id = name
        self.meta = '-'.join(m.groups()[:-1])
        self.nnode = Item.gid2nodes(m[1])
        self.nworker = int(m[2])
        self.portion = float(m[3])
        self.si = float(m[4]) # term-check interval
        self.ci = float(m[5]) # checkpoint interval
        self.ctype = m[6]     # checkpoint type
        self.time = None
        self.ccount = None
    
    def gid2nodes(title):
        if len(title) == 1:
            return 10**int(title)
        elif len(title) == 2:
            return 10**int(title[0]) * int(title[1])
        else:
            print('error in parse graph title:', title)
            return 0
    
    def nodes2gid(nodes):
        f = int(np.log10(nodes))
        r = nodes - 10**f
        if r == 0:
            return str(f)
        else:
            l = int(nodes / 10**f)
            return str(f)+str(l)
        

def merge_time_cp(tdata, cdata):
    mdata = {}
    for n, t in tdata:
        item = Item(n)
        item.time = float(t)
        mdata[n] = item
    for n, c in cdata:
        if n not in mdata:
            item = Item(n)
            mdata[n] = item
        mdata[n].ccount = int(c)
    return mdata


def check_records(records):
    res=([],[])
    for k, v in records.items():
        if v.time is None:
            res[0].append(k)
        if v.ccount is None:
            res[1].append(k)
    return res


def remove_failed_records(records, plist):
    if len(plist[0]) != 0 or len(plist[1]) != 0:
        if len(plist[0]):
            for n in plist[0]:
                records.pop(n)
        if len(plist[1]):
            for n in plist[1]:
                records.pop(n)
    return records

 
OFF_NONE = 5 
OFF_SYNC = 5 + 1
OFF_ASYNC = 5 + 1 + 2
OFF_VS = 5 + 1 + 2 + 2

def transform2table(records, sort=True):
    # format: <meta part> <normal time> <sync part> <async part> <vs part>
    # <meta part>: nodes, workers, portion, termcheck interval, checkpoint interval
    # <sync part>: total time, additional time, number of checkpoint
    res = []
    id2idx = {}
    for n, r in records.items():
        if r.meta in id2idx:
            p = id2idx[r.meta]
        else:
            p = len(res)
            id2idx[r.meta] = p
            res.append([None for i in range(5+1+2+2+2)])
            res[p][0] = r.nnode
            res[p][1] = r.nworker
            res[p][2] = r.portion
            res[p][3] = r.si
            res[p][4] = r.ci
        if r.ctype == 'NONE':
            res[p][OFF_NONE] = r.time
        elif r.ctype == 'SYNC':
            res[p][OFF_SYNC] = r.time
            res[p][OFF_SYNC+1] = r.ccount
        elif r.ctype == 'ASYNC':
            res[p][OFF_ASYNC] = r.time
            res[p][OFF_ASYNC+1] = r.ccount
        else:
            res[p][OFF_VS] = r.time
            res[p][OFF_VS+1] = r.ccount
    #dtype = [('node', int), ('worker', int), ('portion', float),
    #         ('si', float), ('ci', float), ('normal_time', float),
    #         ('sync_time', float), ('sync_cp', int),
    #         ('async_time', float), ('async_cp', int),
    #         ('vs_time', float), ('vs_cp', int)]
    tbl = np.array(res)#, dtype=float)
    #tbl.sort(order=['node','worker','portion','si','ci'])
    if sort:
        for i in range(4,-1,-1):
            tbl = tbl[tbl[:,i].argsort(kind='stable')]
    return tbl


def check_table(tbl):
    loc = np.argwhere(np.isnan(tbl))
    if loc.shape[0] == 0:
        return ([],[])
    res_name = []
    res_row = []
    for l in loc:
        if l[0] not in res_row:
            res_row.append(l[0])
        r = tbl[l[0]]
        a = Item.nodes2gid(r[0])
        if l[1] == OFF_NONE:
            t = 'NONE'
        elif OFF_SYNC <= l[1] < OFF_SYNC + 2:
            t = 'SYNC'
        elif OFF_ASYNC <= l[1] < OFF_ASYNC + 2:
            t = 'ASYNC'
        elif OFF_VS <= l[1] < OFF_VS + 2:
            t = 'VS'
        else:
            continue
        name = 't%s-%d-%g-%g-%g-%s' % (a, r[1], r[2], r[3], r[4], t)
        if name not in res_name:
            res_name.append(name)
    return res_name, res_row


def dump(fn, table, sep=','):
    headers=['nodes', 'worker', 'portion', 'si', 'ci', 'ntime', 
             'sync_t', 'sync_cp', 'async_t', 'async_cp', 'vs_t', 'vs_cp']
    pat = ['%d','%d','%g','%g','%g','%f',
           '%f','%d','%f','%d','%f','%d']
    with open(fn, 'w') as f:
        f.write(sep.join(headers))
        f.write('\n')
        for line in table:
            f.write(sep.join(pat) % tuple(v for v in line))
            f.write('\n')

def main(time_fn, count_fn, smy_fn, rmv_f_rec, rmv_f_exper):
    # record
    tdata = load_time_file(time_fn)
    cdata = load_cp_count_file(count_fn)
    records = merge_time_cp(tdata, cdata)
    print('Load %d records'%len(records))
    failed_list = check_records(records)
    if len(failed_list[0]) != 0 or len(failed_list[1]) != 0:
        print('Found unmatched records')
        if len(failed_list[0]):
            print('No running time:', failed_list[0])
        if len(failed_list[1]):
            print('No checkpoint count:', failed_list[1])
        if rmv_f_rec:
            remove_failed_records(records, failed_list)
            print('After pruning unmatched records, %d left'%len(records))
    # table
    tbl = transform2table(records, True)
    print('Rows of result table: %d'%tbl.shape[0])
    failed_tbl_name, failed_tbl_row = check_table(tbl)
    if len(failed_tbl_name) != 0:
        print('Uncomplete experiments:', failed_tbl_name)
        if rmv_f_exper:
            tbl = np.delete(tbl, failed_tbl_row, 0)
            print('After pruning unmatched rows, %d left'%tbl.shape[0])
    # dump        
    #np.savetxt(smy_fn, tbl, delimiter=',')
    sep = ','
    if smy_fn.endswith('.txt') or smy_fn.endswith('.tsv'):
        sep = '\t'
    dump(smy_fn, tbl, sep)


if __name__ == '__main__':
    argc = len(sys.argv)
    if argc < 3:
        print('Usage: <time_fn> <count_fn> <smy_fn> [rmv_f_record] [rmv_f_exper]')
        print('  <time_fn>: input filename for running time')
        print('  <count_fn>: input filename for running checkpoint count')
        print('  <smy_fn>: output filename for running time')
        print('  [rmv_f_record]: (=True) remove unmatched records')
        print('  [rmv_f_exper]: (=True) remove uncompleted experiment group')
    else:
        rmv_f_record = True if argc <= 4 else sys.argv[4] in ['1','t','T','true']
        rmv_f_exper = True if argc <= 5 else sys.argv[5] in ['1','t','T','true']
        main(sys.argv[1], sys.argv[2], sys.argv[3], rmv_f_record, rmv_f_exper)
    
    
