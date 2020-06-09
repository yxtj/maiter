# -*- coding: utf-8 -*-
"""
Created on Sun Jun  7 23:10:28 2020

@author: yanxi
"""

import re
import numpy as np
import matplotlib.pyplot as plt


def set_small_figure():
    plt.rcParams["figure.figsize"] = [4,3]
    plt.rcParams["font.size"]=12


def set_large_figure():
    plt.rcParams["figure.figsize"] = [6,4.5]
    plt.rcParams["font.size"] = 16


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


def select(data, cond, rel='and'):
    '''
    cond: list of (column, values) pair, values can be a single scalar or a list
    '''
    if cond is None:
        return data
    if isinstance(cond, str):
        cond = makeConditionByName(cond)
    elif not isinstance(cond, list):
        cond = [cond]
    elif len(cond) == 2 and (isinstance(cond[0], int) or isinstance(cond[0], str)):
        cond = [cond]
    assert rel in ['and', 'or']
    for c, vl in cond:
        if isinstance(c, str):
            if c == 'nodes':
                c = 0
            elif c == 'workers':
                c = 1
            elif c == 'portion':
                c = 2
            elif c == 'si':
                c = 3
            elif c == 'ci':
                c = 4
            else:
                c = None
        assert isinstance(c, int)
        if not isinstance(vl, list):
            vl = [vl]
        for v in vl:
            assert isinstance(v, int) or isinstance(v, float)
    if rel == 'and':
        res = data
        for c, vl in cond:
            if not isinstance(vl, list):
                vl = [vl]
            d = res[:,c]
            flag = np.any([d == v for v in vl], 0)
            res = res[flag]
        return res
    else: # 'or'
        flag = np.array([False for i in range(data.shape[0])])
        for c, vl in cond:
            if not isinstance(vl, list):
                vl = [vl]
            d = data[:,c]
            f = np.any([d == v for v in vl], 0)
            flag = np.logical_or(flag, f)
        return data[flag]


def makeCondition(nodes=None, workers=None, portion=None, si=None, ci=None):
    cond = []
    if nodes is not None:
        cond.append((0, nodes))
    if workers is not None:
        cond.append((1, workers))
    if portion is not None:
        cond.append((2, portion))
    if si is not None:
        cond.append((3, si))
    if ci is not None:
        cond.append((4, ci))
    return cond

def makeConditionByName(cond_str):
    pat = '''t(\d+|\*)-(\d+|\*)-(\d+(?:\.\d*)?|\*)-(\d+(?:\.\d*)?|\*)-(\d+(?:\.\d*)?|\*)'''
    m = re.match(pat, cond_str)
    nodes = gid2nodes(m[1]) if m[1] != '*' else None
    workers = int(m[2]) if m[2] != '*' else None
    portion = float(m[3]) if m[3] != '*' else None
    si = float(m[4]) if m[4] != '*' else None
    ci = float(m[5]) if m[5] != '*' else None
    return makeCondition(nodes, workers, portion, si, ci)

def makeXlableTick(ds, nodes=None, workers=None, portion=None, si=None, ci=None):
    xlbl = None
    xticks = None
    found = False
    if nodes is None:
        xlbl = 'number of nodes'
        xticks = ds[:,0]
        found = len(np.unique(xticks)) != 1
    if not found and workers is None:
        xlbl = 'number of workers'
        xticks = ds[:,1]
        found = len(np.unique(xticks)) != 1
    if not found and portion is None:
        xlbl = 'portion'
        xticks = ds[:,2]
        found = len(np.unique(xticks)) != 1
    if not found and si is None:
        xlbl = 'termination check interval'
        xticks = ds[:,3]
        found = len(np.unique(xticks)) != 1
    if not found and ci is None:
        xlbl = 'checkpoint interval'
        xticks = ds[:,4]
    return xlbl, xticks


def drawRunTime(data, width=0.8, xlbl=None, xticks=None, ncol=1, loc=None,
                cond=None, nodes=None, workers=None, portion=None, si=None, ci=None):
    if cond is None:
        cond = makeCondition(nodes, workers, portion, si, ci)
    ds = select(data, cond)
    dp = ds[:,[5,6,8,10]]
    #plt.plot(dp)
    ng = dp.shape[0]
    nb = 4
    barWidth = width/nb
    x = np.arange(ng)
    off = -width/2 + barWidth/2
    plt.figure()
    for i in range(nb):
        y = dp[:,i]
        plt.bar(x + off + barWidth*i, y, barWidth)
    if xlbl is None and xticks is None:
        xlbl, xticks = makeXlableTick(ds, nodes, workers, portion, si, ci)
    plt.xticks(x, xticks)
    plt.xlabel(xlbl)
    plt.ylabel('running time (s)')
    plt.legend(['None','Sync','Async','VS'], ncol=ncol, loc=loc)
    plt.tight_layout()
    
    
    
def drawOverhead(data, average=False, relative=False,
                 width=0.8, xlbl=None,xticks=None, ncol=1, loc=None,
                 cond=None, nodes=None, workers=None, portion=None, si=None, ci=None):
    if cond is None:
        cond = makeCondition(nodes, workers, portion, si, ci)
    ds = select(data, cond)
    dnone = ds[:,5][:,np.newaxis]
    dp = ds[:,[6,8,10]] - dnone
    dp[dp<0] = 0
    dc = ds[:,[7,9,11]]
    if average:
        dp = dp/dc
    if relative:
        dp = dp/dnone
    #plt.plot(dp)
    ng = dp.shape[0]
    nb = 3
    barWidth = width/nb
    x = np.arange(ng)
    off = -width/2 + barWidth/2
    plt.figure()
    for i in range(nb):
        y = dp[:,i]
        plt.bar(x + off + barWidth*i, y, barWidth)
    if xlbl is None and xticks is None:
        xlbl, xticks = makeXlableTick(ds, nodes, workers, portion, si, ci)
    plt.xticks(x, xticks)
    plt.xlabel(xlbl)
    if average:
        if relative:
            ylbl = 'ratio of overhead per checkpoint'
        else:
            ylbl = 'overhead per checkpoint (s)'
    else:
        if relative:
            ylbl = 'ratio of overhead'
        else:
            ylbl = 'overhead time (s)'
    plt.ylabel(ylbl)
    plt.legend(['Sync','Async','VS'], ncol=ncol, loc=loc)
    plt.tight_layout()


def drawScale(data, overhead=False, average=False,
              xscale='nodes', ncol=1, loc=None):
    assert xscale in ['nodes', 'workers']
    if xscale == 'nodes':
        x = data[:,0]
    else:
        x = data[:,1]
    if overhead:
        y0 = data[:,5].reshape(data.shape[0], 1)
        y = data[:,[6,8,10]] - y0
        if average:
            y /= data[:,[7,9,11]]
    else:
        y = data[:,[5,6,8,10]]
    plt.figure()
    plt.plot(x, y)
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel('number of nodes')
    if overhead:
        ylbl = 'overhead per checkpoint (s)' if average else 'overhead time (s)'
        lgd = ['Sync','Async','VS']
    else:
        ylbl = 'running time (s)'
        lgd = ['None', 'Sync','Async','VS']
    plt.ylabel(ylbl)
    plt.legend(lgd, ncol=ncol, loc=loc)
    plt.tight_layout()


def main():
    data=np.loadtxt('../data/res1.txt',delimiter='\t',skiprows=1)
    drawRunTime(data, nodes=5000000, col=1, loc='lower right')
    drawOverhead(data, nodes=1000000, si=2)
    # scale
    dscale = np.vstack([select(data, 't5-*-*-1-5'),
                       select(data, 't52-*-*-1-5'),
                       select(data, 't55-*-*-1-5'),
                       select(data, 't6-*-*-2-5'),
                       select(data, 't62-*-*-5-5'),
                       select(data, 't65-*-*-5-5'),
                       ])
    drawScale(dsale, False)
    drawScale(dsale, True)