class LocalModelBase(object):
    """ 
    Base class for all the serialisations
    """
    view_name = '_all_docs'

    def index_doc(self):
        return {"_id": "_design/index"}

    def state_doc(self):
        return {
            "_id": "_local/state",
            "peers": {}
        }

    def cache_key(self, couch_key):
        return couch_key

    def couch_key(self, cache_key, graph):
        return cache_key

    def get_data(self, doc, subject, graph):
        pass
        
    def add_data(self, couch_doc, cache):
        pass

    def delete_property(self, couch_doc, prop, value):
        pass

    def initial_docs(self):
        """
        TODO: delete
        """
        return [ self.index_doc(), self.state_doc() ]

    def content_doc(self):
        # TODO: check if the method is used
        return {"_id": "_local/content", 'entity_name': []}
    
    def initial_docs_public(self):
        return [ self.index_doc(), self.state_doc() ]
    
    def initial_docs_private(self):
        return [ self.index_doc() ]
    
    def initial_docs_cache(self):
        return [ self.index_doc(), self.content_doc() ]


class ModelS(LocalModelBase):
    """

    Example document:

    {
        "_id": "http://graph.name http://www.w3.org/People/Berners-Lee/card#i",
        "_rev": "1-e004c4ac4b5f7923892ad417d364a85e",
        "http://www.w3.org/2000/01/rdf-schema#label": [
           "Tim Berners-Lee"
        ],
        "http://xmlns.com/foaf/0.1/nick": [
           "TimBL",
           "timbl"
        ]
    }

    """

    @classmethod
    def index_doc(cls):
        return  {
                "_id": "_design/index",
                "views": {
                            "by_entity": {
                                "map": "function(doc) {if ('@id' in doc) {emit(doc['@id'], {'rev': doc._rev, 'g': doc._id})}}"
                            },
                            "by_property_value": {
                            "map": """
                            function(doc) {
                                if ('@id' in doc) {
                                    var entity = doc['@id'];
                                    for (property in doc) {
                                        if (property[0] != '_' && property[0] != '@') {
                                            doc[property].forEach(
                                              function(value) {emit([property, value], entity)}
                                            );
                                        }
                                    }
                                }
                            }
                            """
        }
        }
        }

    # def index_doc(cls):
    #     return  {
    #                 "_id": "_design/index",
    #                 "views": {
    #                     "by_entity": {
    #                         "map": "function(doc) {var a = doc._id.split(' '); if (a.length == 2 && a[1].length > 0) {emit(a[1], {'rev': doc._rev, 'g': a[0]})}}"
    #                     },
    #                     "by_property_value": {
    #                         "map": """function(doc) {
    #                                     var a = doc._id.split(' '); 
    #                                     if (a.length == 2 && a[1].length > 0) {
    #                                       for (property in doc) {
    #                                         if (property[0] != '_') {
    #                                           doc[property].forEach(function(value) {emit([property, value], [a[1], a[0]])});
    #                                         }
    #                                       }
    #                                     }
    #                                   }
    #                                 """
    #                     }
    #                 }
    #             }

    def cache_key(self, couch_key):
        return couch_key.split(' ')[1]

    def couch_key(self, cache_key, graph):
        return "{0} {1}".format(graph, cache_key)

    def delete_property(self, couch_doc, prop, value):
        if prop not in couch_doc:
            return
        
        couch_doc[prop] = filter(lambda a: a != value, couch_doc[prop])

    def add_property(self, couch_doc, prop, value):
        if prop not in couch_doc:
            couch_doc[prop] = []
        couch_doc[prop].append(value)
    
    #      
    # def delete_property(self, couch_doc, prop):
    #     """"
    #      Deletes all values while the new function 
    #      deletes only the given prop value pair
    #     """
    #     couch_doc.pop(prop, [])

    def get_data(self, doc, subject, graph):
        """
        Return property-value dictionary. Call with subject=None, graph=None to get data from a doc without an _id.
        """
        data_dict = doc.copy()
        data_dict.pop('_rev', None)
        doc_id = data_dict.pop('_id', None)
        if doc_id:
            g, s = doc_id.split(' ')
        else:
            g = s = None
        if (subject == s) and ((graph is None) or (graph == g)):
            return data_dict
        return {}

    def add_data(self, couch_doc, cache):
        cache_data = cache[self.cache_key(couch_doc['_id'])]
        for p, oo in cache_data.iteritems():
            couch_doc[p] = couch_doc.get(p, []) + list(oo)
