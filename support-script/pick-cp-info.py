import re
import sys


def cal(str):
    v = re.search(r'\[W0\] -- received type.30 : (\d+)', str)
    if not v:
        return
    n = int(v.group(1)) # number of checkpoints
    # print(n)
    l = re.findall(r'cp_time : (\d+\.\d+)', str)
    s = sum(float(v) for v in l)
    t_all = s / len(l) / n
    # print(s/len(l)/n)
    l = re.findall(r'cp_time_blocked : (\d+\.\d+)', str)
    s = sum(float(v) for v in l)
    # print(s/len(l)/n)
    t_blk = s / len(l) / n
    print(n, t_all, t_blk)


if __main__ == '__main__':
    if len(sys.argv) == 1:
        print('Get the information about checkpoint, including number of CP, averaged CP time, averaged blocked CP time.')
        print('Usage: <log-file-name>*n')
        exit()
    for fn in sys.argv[1:]:
        with open(fn) as fin:
            print(fn)
            data = fin.read()
            t = re.search(r'real (\d+\.\d+)', data)
            if t:
                print(float(t.group(1)))
            n, t_all, t_blk = cal(data)
            print('%d\t%f\t%f' % (n, t_all, t_blk))
