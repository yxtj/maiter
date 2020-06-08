# -*- coding: utf-8 -*-
"""
Created on Sun Jun  7 23:10:28 2020

@author: yanxi
"""

import numpy as np
import matplotlib.pyplot as plt


def set_small_figure():
    plt.rcParams["figure.figsize"] = [4,3]
    plt.rcParams["font.size"]=12


def set_large_figure():
    plt.rcParams["figure.figsize"] = [6,4.5]
    plt.rcParams["font.size"] = 16
    

def nodes2gid(nodes):
    f = int(np.log10(nodes))
    r = nodes - 10**f
    if r == 0:
        return str(f)
    else:
        l = int(nodes / 10**f)
        return str(f)+str(l)


def line2name(line):
    gname = node2gid(line[0])
    return 't%s-%d-%g-%g-%g' % (gname, line[1], line[2], line[3], line[4])


def select(data, cond):
    '''
    cond: list of (column, values) pair, values can be a single scalar or a list
    '''
    if not isinstance(cond, list) or (len(cond) == 2 and isinstance(cond[0], int)):
        cond = [cond]
    for c, vl in cond:
        assert isinstance(c, int)
        if not isinstance(vl, list):
            vl = [vl]
        for v in vl:
            assert isinstance(v, int) or isinstance(v, float)
    res = data
    for c, vl in cond:
        if not isinstance(vl, list):
            vl = [vl]
        d = res[:,c]
        flag = np.any([d == v for v in vl], 0)
        res = res[flag]
    return res


def makeCondition(nodes=None, workers=None, portion=None, si=None, ci=None):
    cond = []
    if nodes is not None:
        cond.append((0, nodes))
    if workers is not None:
        cond.append((1, workers))
    if portion is not None:
        cond.append((2, porrtion))
    if si is not None:
        cond.append((3, si))
    if ci is not None:
        cond.append((4, ci))
    return cond

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


def drawRunTime(data, width=0.8, xlbl=None, xticks=None, ncol=1,
                nodes=None, workers=None, portion=None, si=None, ci=None):
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
        xlbl, xticks = makeXlableTick(data, nodes, workers, portion, si, ci)
    plt.xticks(x, xticks)
    plt.xlabel(xlbl)
    plt.ylabel('running time (s)')
    plt.legend(['None','Sync','Async','VS'], ncol=ncol)
    plt.tight_layout()
    
    
    
def drawOverhead(data, average=False, relative=False,
                 width=0.8, xlbl=None,xticks=None, ncol=1,
                 nodes=None, workers=None, portion=None, si=None, ci=None):
    plt.figure()
    cond = makeCondition(nodes, workers, portion, si, ci)
    ds = select(data, cond)
    dnone = ds[:,5]
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
        xlbl, xticks = makeXlableTick(data, nodes, workers, portion, si, ci)
    plt.xticks(x, xticks)
    plt.xlabel(xlbl)
    if average:
        if relative:
            ylbl = 'ratio of overhead per checkpoint (s)'
        else:
            ylbl = 'overhead time per checkpoint (s)'
    else:
        if relative:
            ylbl = 'ratio of overhead (s)'
        else:
            ylbl = 'overhead time (s)'
    plt.ylabel(ylbl)
    plt.legend(['Sync','Async','VS'], ncol=ncol)
    plt.tight_layout()
    
    

data=np.loadtxt('../data/maiter-pagerank.txt',delimiter='\t',skiprows=1)

