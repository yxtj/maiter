# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 04:19:08 2020

@author: yanxi
"""

import os
import numpy as np

def byte2int(bs):
    res = bs[0] | bs[1]<<8 | bs[2]<<16 | bs[3]<<24
    return res

def byte2int64(bs):
    a = byte2int(bs)
    b = byte2int(bs[4:])
    res = a | b<<32
    return res

def byte2str(bs):
    return ''.join(chr(v) for v in bs)

ASCII_ZERO = ord('0')
def bytestr2int(bs):
    r = 0
    for v in bs:
        r = r*10 + (v - ASCII_ZERO)
    return r

def str2int(ss):
    r = 0
    for v in ss:
        r = r*10 + (ord(v) - ASCII_ZERO)
    return r

def readChunk(data, p):
    n = byte2int(data[p:p+4])
    return (p+4+n, data[p+4:p+4+n])

def readChunkDummy(data, p):
    n = byte2int(data[p:p+4])
    return p+4+n

def readIntChunk(data, p):
    n = byte2int(data[p:p+4])
    return (p+4+n, byte2int(data[p+4:p+4+n]))

def readInt64Chunk(data, p):
    n = byte2int(data[p:p+4])
    return (p+4+n, byte2int64(data[p+4:p+4+n]))

def readStrIntChunk(data, p):
    n = byte2int(data[p:p+4])
    return (p+4+n, bytestr2int(data[p+4:p+4+n]))

def readStrChunk(data, p):
    n = byte2int(data[p:p+4])
    return (p+4+n, byte2str(data[p+4:p+4+n]))

def readHeader(data, p):
    # type, shard-id, num
    p, t = readStrChunk(data, p)
    p, s = readStrIntChunk(data, p)
    p, n = readStrIntChunk(data, p)
    return (p, t, s, n)

def readUnit(data, p):
    # key
    p = readChunkDummy(data, p)
    # v1
    p = readChunkDummy(data, p)
    # v2
    p = readChunkDummy(data, p)
    # v3
    p = readChunkDummy(data, p)
    return p

# %% count functions

def countMsg(fn):
    cnt = 0
    with open(fn, 'rb') as f:
        data = f.read()
        p = 0
        p, t, s, n = readHeader(data, p)
        if t == 'delta':
            cnt += n
        for i in range(n):
            p = readUnit(data, p)
    return cnt


def countMsgAsync(fn):
    cnt = 0
    with open(fn, 'rb') as f:
        data = f.read()
        p = 0
        while p < len(data):
            p = readUnit(data, p)
            cnt += 1
    return cnt
    
# %% estimate


def transGraphSize(fn):
    with open(fn) as f:
        data = f.read()
        nk = data.count('\t')
        nl = data.count(' ')
        return nk, nl

def loadRefSize(path, nworker):
    flist = os.listdir(path)
    size = [(0, 0) for i in range(nworker)]
    for i in range(nworker):
        fn = 'part-%d' % i if 'part-%d' % i in flist else 'part%d' % i
        size[i] = transGraphSize(path+'/'+fn)
    return size


def loadCPSizeSync(path, nworker):
    flist = os.listdir(path)
    size = []
    epoch = 0
    while 'cp-t0-e%d-p0' % epoch in flist:
        pre = path + '/cp-t0-e%d-p' % epoch
        t = [os.path.getsize(pre + str(i)) for i in range(nworker)]
        size.append(t)
        epoch += 1
    return size

def loadCPSizeAsync(path, nworker):
    flist = os.listdir(path)
    size = []
    epoch = 0
    while 'cp-t0-e%d-p0.delta' % epoch in flist:
        pre = path + '/cp-t0-e%d-p' % epoch
        t = [os.path.getsize(pre + str(i) + '.delta') for i in range(nworker)]
        size.append(t)
        epoch += 1
    return size

def calcSizeDiff(nk, nl, cpSize):
    # graph file: 4*nk + 4*nl
    # cp file (graph part): 9*nk+9*nk+9*nk+29*nk = 56*nk
    return cpSize - 56*nk

def estimateMsg(fn, refsize, usize=26):
    size = os.path.getsize(fn)
    return (size - refsize) // usize

def estimate(refPath, cpPath, method, nnode, nworker, usize=26):
    #usize = 9 + 9 + 4 + 4
    #off = nnode//nworker*pad
    refSize = np.array(loadRefSize(refPath, nworker))
    if method == 'sync':
        cpSize = np.array(loadCPSizeSync(cpPath, nworker))
        diff = calcSizeDiff(refSize[:,0], refSize[:,1], cpSize)
    elif method == 'async':
        cpSize = np.array(loadCPSizeAsync(cpPath, nworker))
        diff = cpSize
    nmsg = diff // usize
    return nmsg
    
