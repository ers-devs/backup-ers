import sys, os; sys.path.extend(['/Users/marat/projects/wikireg/site-packages'])
import couchdbkit

hosts = [
'xo-3c-ea-3a.local.',
'xo-0d-58-b2.local.',
'xo-15-4c-93.local.',
'xo-26-7a-e7.local.',
'mmm.local.'
]

urls = ['http://admin:admin@' + str(x) + ':5984'
          for x in hosts]

repl_doc = {'source': 'ers', 'target': 'ers', 'continuous': False, 'create_target': False}

url = urls[-1]
replicator = couchdbkit.Server(url).get_db('_replicator')
# replicator.server.get_or_create_db('ers')
for target_url in urls[:-1]:
    doc = repl_doc.copy()
    doc["source"] = target_url + '/ers'
    doc["_id"] = target_url
    replicator.save_doc(doc, force_update=True)

