# -*- coding: utf-8 -*-
"""
Created on Sun Jun  7 23:10:28 2020

@author: yanxi
"""

import re
import numpy as np
import matplotlib.pyplot as plt

# %% prepare

def set_small_figure(fontsize=12):
    plt.rcParams["figure.figsize"] = [4,3]
    plt.rcParams["font.size"] = fontsize


def set_large_figure(fontsize=16):
    plt.rcParams["figure.figsize"] = [6,4.5]
    plt.rcParams["font.size"] = fontsize


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

# %% data selection

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

# %% plot helper
    
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

# %% plot functions

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
    plt.legend(['None','Sync','Async','FAIC'], ncol=ncol, loc=loc)
    plt.tight_layout()


def drawOverhead(data, average=False, relative=False,
                 width=0.8, xlbl=None,xticks=None, ncol=1, loc=None,
                 cond=None, nodes=None, workers=None, portion=None, si=None, ci=None):
    if cond is None:
        cond = makeCondition(nodes, workers, portion, si, ci)
    ds = select(data, cond)
    dnone = ds[:,5][:,np.newaxis]
    idx = np.array([6,10,8])
    dp = ds[:,idx] - dnone
    dp[dp<0] = 0
    dc = ds[:,idx+1]
    if average:
        dp = dp/dc
    if relative:
        dp = dp/dnone*100
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
            ylbl = 'ratio of overhead (%)'
        else:
            ylbl = 'average overhead (s)'
    else:
        if relative:
            ylbl = 'ratio of overhead (%)'
        else:
            ylbl = 'overhead time (s)'
    plt.ylabel(ylbl)
    plt.legend(['Sync','FAIC','Async'], ncol=ncol, loc=loc)
    plt.tight_layout()

def drawOverheadGroup(data, relative=False, xlbl=None, xticks=None,
                      width=0.6, capsize=8, cond=None,
                      nodes=None, workers=None, portion=None, si=None, ci=None):
    if cond is None:
        cond = makeCondition(nodes, workers, portion, si, ci)
    ds = select(data, cond)
    dnone = ds[:,5][:,np.newaxis]
    idx = np.array([6,10,8])
    dp = ds[:,idx] - dnone
    dp[dp<0] = 0
    dc = ds[:,idx+1]
    d = dp / dc
    if relative:
        d = d/dnone*100
    #plt.plot(d)
    y = d.mean(0)
    err = d.std(0)
    nb = 3
    x = np.arange(nb)
    plt.figure()
    for i in range(nb):
        plt.bar(x[i], y[i], yerr=err[i], width=width, capsize=capsize)
    if xticks is None:
        xticks = ['Sync','FAIC','Async']
    plt.xticks(x, xticks)
    if xlbl is None:
        xlbl = 'checkpoint method'
    plt.xlabel(xlbl)
    if relative:
        ylbl = 'ratio of overhead (%)'
    else:
        ylbl = 'average overhead (s)'
    plt.ylabel(ylbl)
    #plt.legend(['Sync','Async','FAIC'], ncol=ncol, loc=loc)
    plt.tight_layout()


def drawScale(data, overhead=False, average=False, xterm='nodes',
              ref=False, fit=False,
              logx=False, logy=False, xtick=False, ncol=1, loc=None):
    assert xterm in ['nodes', 'workers']
    if xterm == 'nodes':
        x = data[:,0]
    else:
        x = data[:,1]
    idx = np.array([6,10,8])
    if overhead:
        y0 = data[:,5].reshape(data.shape[0], 1)
        y = data[:,idx] - y0
        if average:
            y /= data[:,idx+1]
    else:
        y = data[:,[5,6,10,8]]
    plt.figure()
    plt.plot(x, y)
    if overhead:
        if ref:
            plt.gca().set_prop_cycle(None) # reset color rotation
            r = y[0] * x[0] / x.reshape(-1,1)
            plt.plot(x, r, linestyle='-.')
        if fit:
            plt.gca().set_prop_cycle(None) # reset color rotation
            lx = np.log(x)
            ly = np.log(y)
            for i in range(y.shape[1]):
                a,b = np.polyfit(lx, ly[:,i], 1)
                plt.plot(x, np.exp(a*lx + b), linestyle='--')
    if logx:
        plt.xscale('log')
    if logy:
        plt.yscale('log')
    else:
        plt.ylim(0.0, None)
    if xtick:
        plt.xticks(x, x)
    plt.xlabel('number of ' + xterm)
    if overhead:
        ylbl = 'average overhead (s)' if average else 'overhead time (s)'
        lgd = ['Sync','FAIC','Async']
    else:
        ylbl = 'running time (s)'
        lgd = ['None', 'Sync','FAIC','Async']
    plt.xticks(x, x.astype('int'))
    plt.ylabel(ylbl)
    plt.legend(lgd, ncol=ncol, loc=loc)
    plt.tight_layout()

def drawScaleWorker(data, idx=10, name='FAIC', overhead=True, speedup=False,
                    ref=False, fit=False, refIdx=0,
                    logx=False, logy=False, ncol=1, loc=None):
    x = data[:,1]
    y = data[:,idx]
    if overhead:
        y = (y - data[:,5]) / data[:,idx+1]
        ylbl = 'average overhead (s)'
    else:
        ylbl = 'running time (s)'
    if speedup:
        y = y[refIdx]/y*x[refIdx]
        ylbl = 'speed-up'
    plt.figure()
    lgd = []
    plt.plot(x, y)
    lgd.append(name)
    if ref:
        if speedup:
            r = x
        else:
            r = y[0] * x[0] / x
        plt.plot(x, r, linestyle='-.')
        lgd.append('Ideal')
    if fit:
        if speedup:
            a,b = np.polyfit(x, y, 1)
            f = a*x + b
        else:
            lx = np.log(x)
            ly = np.log(y)
            a,b = np.polyfit(lx, ly, 1)
            f = np.exp(a*lx + b)
        plt.plot(x, f, linestyle='--')
        lgd.append(name+'-ref.')
    if logx:
        plt.xscale('log')
    if logy:
        plt.yscale('log')
    else:
        plt.ylim(0.0, None)
    plt.xlabel('number of worker')
    plt.xticks(x, x.astype('int'))
    plt.ylabel(ylbl)
    plt.grid(True)
    plt.legend(lgd, ncol=ncol, loc=loc)
    plt.tight_layout()
    
# %% main
    
def main():
    data=np.loadtxt('../data/res1.txt',delimiter='\t',skiprows=1)
    drawRunTime(data, nodes=5000000, ncol=1, loc='lower right')
    drawOverhead(data, nodes=1000000, si=2)
    drawOverheadGroup(data, nodes=1000000, si=2)
    # scale
    dscale = np.vstack([select(data, 't5-6-*-1-5'),
                       select(data, 't52-6-*-1-5'),
                       select(data, 't55-6-*-1-5'),
                       select(data, 't6-*6*-2-5'),
                       select(data, 't62-6-*-5-5'),
                       select(data, 't65-6-*-5-5'),
                       ])
    drawScale(dscale, False)
    drawScale(dscale, True, logx=True)