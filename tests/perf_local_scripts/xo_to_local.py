import sys, os; sys.path.extend(['/Users/marat/projects/wikireg/site-packages'])
import requests, time, json, couchdbkit

hosts = ['xo-3c-ea-3a.local.', 'xo-0d-58-b2.local.', 'xo-15-4c-93.local.', 'xo-26-7a-e7.local.']

def check_replicator(host):
    target = "http://admin:admin@localhost:5984/replicator-{0}".format(host.split('.')[0])
    repl_doc = {
       "_id": "check-{0}-replicator".format(host),
       "source": "http://admin:admin@{0}:5984/_replicator".format(host),
       "target": target,
       "create_target": True,
       "continuous": False
    }
    # requests.put(target)
    replicator = couchdbkit.Server("http://admin:admin@localhost:5984").get_db('_replicator')
    replicator.save_doc(repl_doc, force_update=True)

for host in hosts:
    check_replicator(host)

