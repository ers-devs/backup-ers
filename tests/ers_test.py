import couchdbkit
from os.path import join
from ers import ERSLocal, ModelS, ModelT

def test():
    server = couchdbkit.Server(r'http://admin:admin@127.0.0.1:5984/')
    def prepare_ers(model, dbname='ers_test'):
        if dbname in server:
            server.delete_db(dbname)
        ers = ERSLocal(dbname=dbname, model=model)
        ers.import_nt(join('data','timbl.nt'), 'timbl')
        return ers

    def test_ers():
        """Model independent tests"""
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'bad_graph') == False
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
        assert ers.delete_entity('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl')['ok'] == True
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == False
        s = 'urn:ers:meta:testEntity'
        p = 'urn:ers:meta:predicates:hasValue'
        g = 'urn:ers:meta:testGraph'
        objects = set(['value 1', 'value 2'])
        for o in objects:
            ers.add_data(s, p, o, g)
        data = ers.get_data(s, g)
        assert set(data[p]) == objects
        assert set(ers.get_value(s, p, g)) == objects

    for model in [ModelS(), ModelT()]:
        dbname = 'ers_' + model.__class__.__name__.lower()
        ers = prepare_ers(model, dbname)
        test_ers()
 
    print "Tests pass"

test()

