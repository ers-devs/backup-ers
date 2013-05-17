import sys, os; sys.path.extend(['/Users/marat/projects/wikireg/site-packages'])
import requests, time

hosts = ['xo-0d-58-b2.local.', 'xo-15-4c-93.local.', 'xo-26-7a-e7.local.', 'xo-3c-ea-3a.local.']
# hosts = ['xo-0d-58-b2.local.', 'xo-15-4c-93.local.', 'xo-3c-ea-3a.local.']

bounds = sorted(hosts) + ['xo-g']
payload = {'limit': 0}

def doc_stats(host):
    target_url = 'http://admin:admin@{0}:5984/xo-ers/_all_docs'.format(host)
    offsets = []
    for bound in bounds:
        payload['startkey'] = '"{0}"'.format(bound)
        r = requests.get(target_url, params=payload).json()
        offsets.append(r['offset'])
    stats = [r['total_rows']] + [offsets[i+1]-offsets[i] for i in range(len(offsets)-1)]
    return stats

def host_stats(host):
    t0 = time.time()
    doc_counts = doc_stats(host)
    t1 = time.time()
    call_time = t1 - t0
    return doc_counts + [call_time, t1, host]

fname = sys.argv[1] if len(sys.argv)>1 else 'xo-mon.txt'
fout = open(fname, 'a')
def printout(line):
    l = "{0}\n".format(line)
    sys.stdout.write(l)
    fout.write(l)
    sys.stdout.flush()

printout("=" * 70)
printout("total_docs\t{0}\tcall_time\ttimestamp\thost".format('\t'.join(bounds[:-1])))

while True:
    stats = '\t'.join(map(str, host_stats('mmm.local.')))
    printout(stats)
    time.sleep(5)

