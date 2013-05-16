import sys, os; sys.path.extend(['/Users/marat/projects/wikireg/site-packages'])
import couchdbkit

urls = ['http://admin:admin@10.20.52.' + str(x) + ':5984'
          for x in (129, 130, 140, 151)]

repl_doc = {'source': 'ers', 'target': 'ers', 'continuous': True, 'create_target': True}

for url in urls:
    replicator = couchdbkit.Server(url).get_db('_replicator')
    for target_url in urls:
        doc = repl_doc.copy()
        doc["target"] = target_url + '/ers'
        replicator.save_doc(doc)
