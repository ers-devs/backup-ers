#!/usr/bin/python

import couchdbkit
import rdflib
import peer_monitor
import threading
import time

from StringIO import StringIO
from collections import defaultdict
from models import ModelS, ModelT
from global_server_interface import GlobalServerInterface, GlobalServerAccessException

# Document model is used to store data in CouchDB. The API is independent from the choice of model.
DEFAULT_MODEL = ModelS()


def merge_annotations(a, b):
    for key in set(a.keys() + b.keys()):
        a.setdefault(key, []).extend(b.get(key, []))


class EntityCache(defaultdict):
    """Equivalent to defaultdict(lambda: defaultdict(set))."""
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, e, p, v):
        """Add <e, p, v> to cache."""
        self[e][p].add(v)

    def iter_triples(self):
        for e, pv in self.items():
            for p, values in pv.items():
                for v in values:
                    yield e, p, v

    def parse_nt(self, **kwargs):
        if 'filename' in kwargs:
            lines = open(kwargs['filename'], 'r')
        elif 'data' in kwargs:
            lines = StringIO(kwargs['data'])
        else:
            raise RuntimeError("Must specify filename= or data= for parse_nt")

        for input_line in lines:
            triple = input_line.split(None, 2) # assumes SPO is separated by any whitespace string with leading and trailing spaces ignored
            s = triple[0][1:-1] # get rid of the <>, naively assumes no bNodes for now
            p = triple[1][1:-1] # get rid of the <>
            o = triple[2][1:-1] # get rid of the <> or "", naively assumes no bNodes for now
            oquote = triple[2][0]
            if oquote == '"':
                o = triple[2][1:].rsplit('"')[0]
            elif oquote == '<':
                o = triple[2][1:].rsplit('>')[0]
            else:
                o = triple[2].split(' ')[0] # might be a named node

            self.add(s, p, o)

        return self

    def parse_nt_rdflib(self, **kwargs):
        graph = rdflib.Graph()

        if 'filename' in kwargs:
            graph.parse(location=kwargs['filename'], format='nt')
        elif 'data' in kwargs:
            graph.parse(data=kwargs['data'], format='nt')
        else:
            raise RuntimeError("Must specify filename= or data= for parse_nt_rdflib")

        for s, p, o in graph:
            self.add(str(s), str(p), str(o))

        return self


class ERSReadOnly(object):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model=DEFAULT_MODEL):
        self.server = couchdbkit.Server(server_url)
        self.db = self.server.get_db(dbname)
        self.model = model

    def get_data(self, subject, graph=None):
        """get all property+values for an identifier"""
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            merge_annotations(result, self.model.get_data(doc, subject, graph))

        return result

    def get_doc(self, subject, graph):
        try:
            return self.db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound:
            return None

    def get_values(self, subject, predicate, graph=None):
        """ Get the value for a identifier+property (return null or a special value if it does not exist)
            Return a list of values or an empty list
        """
        data = self.get_data(subject, graph)

        return data.get(predicate, [])

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))


class ERSReadWrite(ERSReadOnly):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers', model=DEFAULT_MODEL):
        self.server = couchdbkit.Server(server_url)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model
        if not self.db.doc_exist('_design/index'):
            self.db.save_doc(self.model.index_doc())

    def add_data(self, s, p, o, g):
        """Adds the value for the given property in the given entity. Create the entity if it does not exist yet)"""
        triples = EntityCache()
        triples.add(s, p, o)
        self.write_cache(triples, g)

    def delete_entity(self, entity, graph=None):
        """Deletes the entity."""
        # Assumes there is only one entity per doc.
        if graph is None:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True}
                    for r in self.db.view('index/by_entity', key=entity)]
        else:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True}
                    for r in self.db.view('index/by_entity', key=entity)
                    if r['value']['g'] == graph]

        return self.db.save_docs(docs)

    def delete_value(self, entity, prop, graph=None):
        """Deletes all of the user's values for the given property in the given entity."""
        if graph is None:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)]
        else:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)
                             if r['value']['g'] == graph]
        for doc in docs:
            self.model.delete_property(doc, prop)

        return self.db.save_docs(docs)

    def import_nt(self, file_name, target_graph):
        """Import N-Triples file."""
        cache = EntityCache().parse_nt(filename=file_name)

        self.write_cache(cache, target_graph)

    def import_nt_rdflib(self, file_name, target_graph):
        """Import N-Triples file using rdflib."""
        # TODO: get rid of the intermediate cache?
        cache = EntityCache().parse_nt_rdflib(filename=file_name)

        self.write_cache(cache, target_graph)

    def write_cache(self, cache, graph):
        docs = []
        # TODO: check if sorting keys makes it faster
        couch_docs = self.db.view(self.model.view_name, include_docs=True,
                                  keys=[self.model.couch_key(k, graph) for k in cache])
        for doc in couch_docs:
            couch_doc = doc.get('doc', {'_id': doc['key']})
            self.model.add_data(couch_doc, cache)
            docs.append(couch_doc)
        self.db.save_docs(docs)

    def update_value(self, subject, object, graph=None):
        """update a value for an identifier+property (create it if it does not exist yet)"""
        pass


class ERSLocal(ERSReadWrite):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model=DEFAULT_MODEL,
                fixed_peers=()):
        super(ERSLocal, self).__init__(server_url, dbname, model)
        self.fixed_peers = list(fixed_peers)

    def get_annotation(self, entity, graph=None):
        result = self.get_data(entity, graph)
        for remote in self.get_peer_ers_interfaces():
            merge_annotations(result, remote.get_data(entity, graph))

        return result

    def get_values(self, entity, prop, graph=None):
        entity_data = self.get_annotation(entity, graph)

        return entity_data.get(prop, [])

    def get_peer_ers_interfaces(self):
        result = []

        for peer_info in self.fixed_peers + peer_monitor.get_peers():
            if 'url' in peer_info:
                server_url = peer_info['url']
            elif 'host' in peer_info and 'port' in peer_info:
                server_url = r'http://admin:admin@' + peer_info['host'] + ':' + str(peer_info['port']) + '/'
            else:
                continue

            dbname = peer_info['dbname'] if 'dbname' in peer_info else self.dbname

            peer_ers = ERSReadOnly(server_url, dbname)
            result.append(peer_ers)

        return result


class ERSGlobal(object):
    server = None

    def __init__(self, server_url=r'http://localhost:8888/'):
        self.server = GlobalServerInterface(server_url)

    def get_annotation(self, entity, graph=None):
        return self.get_data(entity, graph)

    def get_data(self, entity, graph=None):
        if graph is not None and not self.server.graph_exists(graph):
            return {}

        args = {'e': entity}
        if graph is not None:
            args['g'] = graph

        annotation = {}
        for e, p, v, g in self.server.query(**args):
            if e != entity or (graph is not None and g != graph):
                continue

            if p not in annotation:
                annotation[p] = list()

            annotation[p].append(v)

        return annotation

    def get_values(self, entity, prop, graph=None):
        if graph is not None and not self.server.graph_exists(graph):
            return []

        args = {'e': entity, 'p': prop}
        if graph is not None:
            args['g'] = graph

        values = []
        for e, p, v, g in self.server.query(**args):
            if e != entity or p != prop or (graph is not None and g != graph):
                continue

            values.append(v)

        return values

    def exist(self, entity, graph):
        if not self.server.graph_exists(graph):
            return False

        return self.server.exist(e=entity, g=graph)

    def add_data(self, entity, prop, value, graph):
        if not self.server.graph_exists(graph):
            self.server.create_empty_graph(graph)

        self.server.create(entity, prop, value, graph)

    def delete_entity(self, entity, graph=None):
        if graph is not None and not self.server.graph_exists(graph):
            return

        args = {'e': entity}
        if graph is not None:
            args['g'] = graph

        self.server.delete(**args)

    def delete_value(self, entity, prop, graph=None):
        if graph is not None and not self.server.graph_exists(graph):
            return

        args = {'e': entity, 'p': prop}
        if graph is not None:
            args['g'] = graph

        self.server.delete(**args)

    def import_nt(self, file_name, target_graph):
        cache = EntityCache().parse_nt(filename=file_name)

        self.write_cache(cache, target_graph)

    def update_value(self, entity, prop, new_value, graph):
        self.server.delete(e=entity, p=prop, g=graph)
        self.server.create(entity, prop, new_value, graph)

    def write_cache(self, cache, graph):
        if not self.server.graph_exists(graph):
            self.server.create_empty_graph(graph)

        for e, p, v in cache.iter_triples():
            self.server.create(e, p, v, graph)

    def check_server_online_now(self):
        try:
            self.server.graph_exists('urn:ers:meta:bogusGraph')

            return True
        except GlobalServerAccessException:
            return False


GLOBAL_SERVER_PING_INTERVAL_SEC = 10.0


class ERS:
    local_ers = None
    global_ers = None

    _monitor_thread = None
    _server_online_now = False

    def __init__(self, local_server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model=DEFAULT_MODEL,
                 global_server_url=r'http://127.0.0.1:8888/', fixed_peers=()):

        self.local_ers = ERSLocal(local_server_url, dbname, model, fixed_peers)
        self.global_ers = ERSGlobal(global_server_url)

        self._monitor_thread = threading.Thread(target=self._run_monitor_thread)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

    def get_annotation(self, entity, graph=None):
        result = self.local_ers.get_annotation(entity, graph)

        if self._server_online_now:
            try:
                merge_annotations(result, self.global_ers.get_annotation(entity, graph))
            except RuntimeError:
                pass

        return list(result)

    def get_values(self, entity, prop, graph=None):
        result = set(self.local_ers.get_values(entity, prop, graph))

        if self._server_online_now:
            try:
                result |= self.global_ers.get_values(entity, graph)
            except RuntimeError:
                pass

        return list(result)

    def exist(self, entity, graph):
        result = self.local_ers.exist(entity, graph)

        if self._server_online_now:
            try:
                result = result or self.global_ers.exist(entity, graph)
            except RuntimeError:
                pass

        return result

    def add_data(self, entity, prop, value, graph):
        self.local_ers.add_data(entity, prop, value, graph)

    def delete_entity(self, entity, graph=None):
        self.local_ers.delete_entity(entity, graph)

    def delete_value(self, entity, prop, graph=None):
        self.local_ers.delete_value(entity, prop, graph)

    def import_nt(self, file_name, target_graph):
        self.local_ers.import_nt(file_name, target_graph)

    def update_value(self, entity, prop, new_value, graph):
        self.local_ers.update_value(entity, prop, new_value, graph)

    def write_cache(self, cache, graph):
        self.local_ers.write_cache(cache, graph)

    def _run_monitor_thread(self):
        while True:
            time.sleep(GLOBAL_SERVER_PING_INTERVAL_SEC)
            self._server_online_now = self.global_ers.check_server_online_now()


def test():
    server = couchdbkit.Server(r'http://admin:admin@127.0.0.1:5984/')

    def create_ers(dbname, model=DEFAULT_MODEL):
        if dbname in server:
            server.delete_db(dbname)           
        ers_new = ERSLocal(dbname=dbname, model=model)
        return ers_new
 
    def test_ers():
        """Model independent tests"""
        ers.import_nt('../../tests/data/timbl.nt', 'timbl')
        assert ers.db.doc_exist('_design/index')
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'bad_graph') == False
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
        ers.delete_entity('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl')
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == False
        for o in objects:
            ers.add_data(s, p, o, g)
            ers.add_data(s, p2, o, g)
        for o in objects2:
            ers.add_data(s, p, o, g2)
            ers.add_data(s, p2, o, g2)
        data = ers.get_data(s, g)
        assert set(data[p]) == objects
        data2 = ers.get_data(s) # get data from all graphs
        assert set(data2[p]) == local_objects
        ers.delete_value(entity, p2)
        assert p2 not in ers.get_annotation(entity)


    # Test data
    s = entity = 'urn:ers:meta:testEntity'
    p = 'urn:ers:meta:predicates:hasValue'
    p2 = 'urn:ers:meta:predicates:property'
    g = 'urn:ers:meta:testGraph'
    g2 = 'urn:ers:meta:testGraph2'
    g3 = 'urn:ers:meta:testGraph3'
    objects = set(['value 1', 'value 2'])
    objects2 = set(['value 3', 'value 4'])
    local_objects = objects | objects2
    remote_objects = set(['value 5', 'value 6'])
    all_objects = local_objects | remote_objects

    # Test local ers using differend document models
    for model in [ModelS(), ModelT()]:
        dbname = 'ers_' + model.__class__.__name__.lower()
        ers = create_ers(dbname, model)
        test_ers()

    # Prepare remote ers
    ers_remote = create_ers('ers_remote')
    for o in remote_objects:
        ers_remote.add_data(entity, p, o, g3)

    # Query remote
    ers_local = ERSLocal(dbname='ers_models', fixed_peers=[{'url': r'http://admin:admin@127.0.0.1:5984/',
                                                            'dbname': 'ers_remote'}])
    assert set(ers_local.get_annotation(entity)[p]) == all_objects
    assert set(ers_local.get_values(entity, p)) == all_objects
    ers_local.delete_entity(entity)
    assert set(ers_local.get_annotation(entity)[p]) == remote_objects

    print "Tests pass"


if __name__ == '__main__':
    test()

