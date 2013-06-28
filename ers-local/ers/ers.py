#!/usr/bin/python

import couchdbkit
import rdflib
import peer_monitor

from StringIO import StringIO
from collections import defaultdict
from models import ModelS, ModelT
                                                        
# Document model is used to store data in CouchDB. The API is independent from the choice of model.
DEFAULT_MODEL = ModelS()


def merge_annotations(a, b):
    for key in set(a.keys() + b.keys()):
        a.setdefault(key, []).extend(b.get(key, []))


class EntityCache(defaultdict):
    """Equivalent to defaultdict(lambda: defaultdict(set))."""
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, s, p, o):
        """Add <s, p, o> to cache."""
        self[s][p].add(o)

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

    def search(self, prop, value=None):
        """ Search entities by property or property+value
            Return a list of [entity, graph] pairs.
        """
        if value is None:
            result = [r['value'] for r in self.db.view('index/by_property_value', startkey=[prop], endkey=[prop, {}])]
        else:
            result = [r['value'] for r in self.db.view('index/by_property_value', key=[prop, value])]
        return result      

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))


class ERSReadWrite(ERSReadOnly):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers', model=DEFAULT_MODEL, reset_database=False):
        self.server = couchdbkit.Server(server_url)
        if reset_database and dbname in self.server:
            self.server.delete_db(dbname)
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
                fixed_peers=(), reset_database=False):
        super(ERSLocal, self).__init__(server_url, dbname, model, reset_database)
        self.fixed_peers = list(fixed_peers)

    def get_annotation(self, entity):
        result = self.get_data(entity)
        for remote in self.get_peer_ers_interfaces():
            merge_annotations(result, remote.get_data(entity))
        return result

    def get_values(self, entity, prop):
        entity_data = self.get_annotation(entity)
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


if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."

