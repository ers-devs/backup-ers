import sys, os; sys.path.extend(['/Users/marat/projects/wikireg/site-packages'])
import couchdbkit
import socket

hosts = ['xo-0d-58-b2.local.', 'xo-15-4c-93.local.', 'xo-26-7a-e7.local.', 'xo-3c-ea-3a.local.']
# hosts = ['xo-0d-58-b2.local.', 'xo-15-4c-93.local.', 'xo-26-7a-e7.local.']
# hosts = ['xo-0d-58-b2.local.', 'xo-15-4c-93.local.']
hosts = [socket.gethostbyname(h) for h in hosts]

urls = ['http://admin:admin@' + str(x) + ':5984'
          for x in hosts]

repl_doc = {'source': 'ers', 'target': 'ers', 'continuous': True, 'create_target': False}

for url in urls:
    replicator = couchdbkit.Server(url).get_db('_replicator')
    replicator.server.get_or_create_db('ers')
    for target_url in urls:
        doc = repl_doc.copy()
        doc["target"] = target_url + '/ers'
        repl_id = url[19:30] + '_to_' + target_url[19:30]
        doc["_id"] = repl_id
        print repl_id, replicator.save_doc(doc, force_update=True)['ok']
