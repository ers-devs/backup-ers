#!/usr/bin/python

import couchdbkit
import re
import sys

from models import ModelS, ModelT
from utils import EntityCache

DEFAULT_MODEL = ModelS()

class ERSReadOnly(object):
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False):
        self.local_only = local_only
        self.server = couchdbkit.Server(server_url)
        self.model = model
        self.fixed_peers = list(fixed_peers)
        self.db = self.server.get_db(dbname)

    def get_annotation(self, entity):
        result = self.get_data(entity)
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'], local_only=True)
                remote_result = remote.get_data(entity)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue
            self._merge_annotations(result, remote_result)
        return result

    def get_data(self, subject, graph=None):
        """get all property+values for an identifier"""
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            self._merge_annotations(result, self.model.get_data(doc, subject, graph))
        return result

    def get_doc(self, subject, graph):
        try:
            return self.db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound: 
            return None

    def get_values(self, entity, prop):
        """ Get the value for a identifier+property (return null or a special value if it does not exist)
            Return a list of values or an empty list
        """
        entity_data = self.get_annotation(entity)
        return entity_data.get(prop, [])

    def search(self, prop, value=None):
        """ Search entities by property or property+value
            Return a list of unique (entity, graph) pairs.
        """
        if value is None:
            view_range = {'startkey': [prop], 'endkey': [prop, {}]}
        else:
            view_range = {'key': [prop, value]}
        result = set([tuple(r['value']) for r in self.db.view('index/by_property_value', **view_range)])
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'])
                remote_result = remote.search(prop, value)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue
            result.update(remote_result)
        return list(result)

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))

    def get_peers(self):
        result = []
        if self.local_only:
            return result
        for peer_info in self.fixed_peers:
            if 'url' in peer_info:
                server_url = peer_info['url']
            elif 'host' in peer_info and 'port' in peer_info:
                server_url = r'http://admin:admin@' + peer_info['host'] + ':' + str(peer_info['port']) + '/'
            else:
                raise RuntimeError("Must include either 'url' or 'host' and 'port' in fixed peer specification")

            dbname = peer_info['dbname'] if 'dbname' in peer_info else 'ers'

            result.append({'server_url': server_url, 'dbname': dbname})
        state_doc = self.db.open_doc('_local/state')
        for peer in state_doc['peers']:
            result.append({
                'server_url': r'http://admin:admin@' + peer['ip'] + ':' + str(peer['port']) + '/',
                'dbname': peer['dbname']
            })
        return result

    def _merge_annotations(self, a, b):
        for key, values in b.iteritems():
            unique_values = set(values)
            unique_values.update(a.get(key,[]))
            a[key] = list(unique_values)


class ERSLocal(ERSReadOnly):
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False,
                 reset_database=False):
        self.local_only = local_only
        self.server = couchdbkit.Server(server_url)
        if reset_database and dbname in self.server:
            self.server.delete_db(dbname)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model
        for doc in self.model.initial_docs():
            if not self.db.doc_exist(doc['_id']):
                self.db.save_doc(doc)
        self.fixed_peers = list(fixed_peers)

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
        raise NotImplementedError


if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."
