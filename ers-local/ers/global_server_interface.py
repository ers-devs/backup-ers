import urllib
import urllib2
import rdflib
import re
import htmlentitydefs
import mimetools


URN_PREFIX_RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
URN_PREFIX_RDFG = 'http://www.w3.org/2004/03/trix/rdfg-1/'

URN_RDF_TYPE = URN_PREFIX_RDF + 'type'
URN_RDFG_GRAPH = URN_PREFIX_RDFG + 'Graph'


def html_entity_decode(text):
    def replace_fn(m):
        text = m.group(0)
        if text[:2] == "&#":
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass

        return text

    return re.sub("&#?\w+;", replace_fn, text)


def encode_rdflib_term(term):
    if re.match(r'^[a-z]+:', unicode(term), re.I):
        return rdflib.URIRef(term)
    else:
        return rdflib.Literal(term)


def decode_rdflib_term(term):
    if isinstance(term, rdflib.URIRef):
        return unicode(term)
    else:
        term = term.toPython()
        if isinstance(term, rdflib.Literal):
            return unicode(term)
        else:
            return term

def parse_rdflib_term(text):
    # TODO: language tags and RDF types not supported yet
    try:
        if text[0] == '<' and text[-1] == '>':
            return rdflib.URIRef(text[1:-1])
        if text[0] == '"' and text[-1] == '"':
            return rdflib.Literal(text[1:-1].decode('string_escape'))

        raise RuntimeError()
    except:
        raise RuntimeError("Error parsing RDF term '{0}'".format(text))


class GlobalServerInterface(object):
    """
    An interface to the CumulusRDF API as exposed by the global server.

    This class and its methods will change as the server API evolves.
    """

    _server_url = None

    timeout_sec = 6.0

    def __init__(self, server_url):
        if not server_url.endswith('/'):
            server_url += '/'

        self._server_url = server_url

    def graph_exists(self, graph):
        return self._do_bool_request('exist_graph', {'g': graph})

    def add_graph_info(self, graph, prop, value):
        self._do_request('graph', {'g_id': graph, 'g_p': prop, 'g_v': value}, 'POST')

    def create_empty_graph(self, graph):
        self.add_graph_info(graph, URN_RDF_TYPE, URN_RDFG_GRAPH)

    def delete_graph(self, graph, force=False):
        self._do_request('graph', {'g': graph, '#f': 'y' if force else 'n'}, 'DELETE')

    def create(self, entity, prop, value, graph):
        self._do_request('create', {'e': entity, 'p': prop, 'v': value, 'g': graph}, 'POST')

    def update(self, entity, prop, old_value, new_value, graph):
        self._do_request('update', {'e': entity, 'p': prop, 'v_old': old_value, 'v_new': new_value, 'g': graph}, 'POST')

    def delete(self, **kwargs):
        params = dict((k, v) for k, v in kwargs.items() if k in ['e', 'p', 'v', 'g'])

        self._do_request('delete', params, 'DELETE')

    def query_all_graphs(self, limit):
        return self._do_quads_request('query_all_graphs', {'#limit': limit})

    def query_graph(self, graph):
        return self._do_quads_request('query_graph', {'g': graph})

    def query(self, **kwargs):
        params = dict((k, v) for k, v in kwargs.items() if k in ['e', 'p', 'v', 'g'])

        try:
            tuples = self._do_quads_request('query', params)

            return tuples
        except GlobalServerOperationException as e:
            if 'resource not found' in str(e):
                return []
            else:
                raise e

    def exist(self, **kwargs):
        params = dict((k, v) for k, v in kwargs.items() if k in ['e', 'p', 'v', 'g'])

        return self._do_bool_request('exist_entity', params)

    def bulk_load(self, graph, **kwargs):
        data = None
        if 'file' in kwargs:
            with open(kwargs['file'], 'r') as f:
                data = f.read()
        elif 'triples' in kwargs:
            data = ''.join(' '.join(encode_rdflib_term(x).n3() for x in tup) + '.\n'
                           for tup in kwargs['triples'])
        elif 'data' in kwargs:
            data = kwargs['data']

        if data is None:
            raise GlobalServerOperationException("You must specify file=, data= or triples= with bulk_load()")

        boundary = mimetools.choose_boundary()

        req_url = self._server_url + 'bulkload'

        req_data = '\r\n'.join([
            '--' + boundary,
            'Content-Disposition: form-data; name="g"',
            '',
            encode_rdflib_term(graph).n3(),
            '--' + boundary,
            'Content-Disposition: form-data; name="filedata"; filename="tuples.nt"',
            'Content-Type: application/octet-stream',
            '',
            data,
            '--' + boundary + '--',
            ''
        ])

        request = RequestEx(req_url, req_data)
        request.set_method('POST')
        request.add_header('Accept', 'text/plain')
        request.add_header('Content-Type', 'multipart/form-data; boundary=' + boundary)
        request.add_header('Content-Length', len(req_data))

        response = self._do_http_request(request)

    def _do_request(self, operation, params, method='GET'):
        req_url = self._server_url + urllib.quote_plus(operation)
        req_data = None

        if method == 'POST':
            req_data = self._encode_params(params)
        else:
            req_url = req_url + '?' + self._encode_params(params)

        request = RequestEx(req_url, req_data, method=method)
        request.add_header('Accept', 'text/plain')

        return self._do_http_request(request)

    def _do_http_request(self, request):
        http_exception = None

        try:
            response = urllib2.urlopen(request, None, self.timeout_sec).read()
        except urllib2.HTTPError as e:
            response = e.read()
            http_exception = e
        except urllib2.URLError as e:
            raise GlobalServerAccessException(str(e))
        except Exception as e:
            raise GlobalServerAccessException(str(e))

        match = re.match('(OK|ERROR) /[^ ]* \d+: ([^\n]*)\n?', response, re.S)
        if match is None:
            if http_exception is not None:
                raise GlobalServerAccessException(str(http_exception))
            else:
                raise GlobalServerInternalException("Received malformed response from server:\n" + response)

        status = match.group(1)
        message = match.group(2)
        data = response[match.end():]

        if status == 'ERROR':
            raise GlobalServerOperationException(message)

        if data != "":
            return message + '\n' + data
        else:
            return message

    # TODO: Eventually we should distinguish internally between literal URLs and URIRefs. Till then...
    def _encode_params(self, params):
        assignments = []

        for k, v in params.items():
            if k.startswith('#'):
                assignments.append((k[1:], str(v)))
            else:
                assignments.append((k, encode_rdflib_term(v).n3()))

        return urllib.urlencode(assignments)

    def _do_bool_request(self, operation, params):
        response = self._do_request(operation, params)

        if response.startswith('TRUE'):
            return True
        elif response.startswith('FALSE'):
            return False
        else:
            raise GlobalServerInternalException(
                "Invalid response to {0}; expected TRUE or FALSE, got '{0}'".format(operation, response))

    def _do_quads_request(self, operation, params):
        PAT_N3_VALUE = r'(<[^>]+>|"[^"]*"[^ ]*|_[^ ]+)'
        PAT_QUADS_LINE = '{0} {0} {0} . {0}'.format(PAT_N3_VALUE)

        response = self._do_request(operation, params)

        quads = []
        for line in response.split('\n')[1:]:
            if line == "":
                continue

            match = re.match(PAT_QUADS_LINE, line, re.I)
            if match is None:
                raise GlobalServerInternalException("Malformed line when receiving quads response:\n" + line)

            quads.append([decode_rdflib_term(parse_rdflib_term(match.group(i))) for i in xrange(1,5)])

        return quads


class GlobalServerAccessException(RuntimeError):
    """Thrown when the global server is not accessible at the given URL.
       Repeating the command at a later time may work."""
    def __init__(self, message='Unspecified error'):
        RuntimeError.__init__(self, 'Cannot access global server: ' + message)


class GlobalServerOperationException(RuntimeError):
    """Thrown when there is an error with the operation itself (missing parameter, conflicts, resource exists etc.)"""
    def __init__(self, message='Unspecified error'):
        RuntimeError.__init__(self, 'Error performing operation on global server: ' + message)


class GlobalServerInternalException(RuntimeError):
    """Thrown when the operation should be valid but the server behaves unexpectedly"""
    def __init__(self, message='Unspecified error'):
        RuntimeError.__init__(self, 'Internal error in global server: ' + message)


class RequestEx(urllib2.Request):
    _method_override = None

    def __init__(self, *args, **kwargs):
        if 'method' in kwargs:
            self._method_override = kwargs['method']
            del kwargs['method']

        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method_override if self._method_override is not None else urllib2.Request.get_method(self)

    def set_method(self, method):
        self._method_override = method


def test():
    TEST_GRAPHS = ['ers:testGraph1', 'ers:testGraph2', 'ers:testGraph3', 'ers:testGraphBulk']

    TEST_QUADS = [
        ['ers:testEntity1', 'ers:testProp1', 'testValue1', 'ers:testGraph1'],
        ['ers:testEntity1', 'ers:testProp2', 'ers:testValue2', 'ers:testGraph1'],
        ['ers:testEntity1', 'ers:testProp3', 'ers:testValue31', 'ers:testGraph1'],
        ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32', 'ers:testGraph1'],
        ['ers:testEntity2', 'ers:testProp1', 'testValue1', 'ers:testGraph1'],
        ['ers:testEntity2', 'ers:testProp2', 'ers:testValue2', 'ers:testGraph1'],
        ['ers:testEntity2', 'ers:testProp3', 'ers:testValue31', 'ers:testGraph1'],
        ['ers:testEntity2', 'ers:testProp3', 'ers:testValue32', 'ers:testGraph1'],
        ['ers:testEntity1', 'ers:testProp1', 'testValue1', 'ers:testGraph2'],
        ['ers:testEntity1', 'ers:testProp4', 'ers:testValue2', 'ers:testGraph2'],
        ['ers:testEntity1', 'ers:testProp3', 'ers:testValue31', 'ers:testGraph2'],
        ['ers:testEntity1', 'ers:testProp3', 'ers:testValue34', 'ers:testGraph2'],
        ['ers:testEntity4', 'ers:testProp1', 'testValue5', 'ers:testGraph2'],
        ['ers:testEntity4', 'ers:testProp2', 'ers:testValue6', 'ers:testGraph2'],
        ['ers:testEntity4', 'ers:testProp4', 'ers:testValue31', 'ers:testGraph2'],
        ['ers:testEntity4', 'ers:testProp4', 'ers:testValue32', 'ers:testGraph2']
    ]

    def unique(l):
        if l is None:
            return None

        result = []
        for item in sorted(l):
            if len(result) == 0 or item != result[-1]:
                result.append(item)

        return result

    def same(tuples, result_tuples):
        tuples = unique(tuples)
        result_tuples = unique(result_tuples)

        if tuples != result_tuples:
            print "Got:"
            print '\n'.join('    ' + ' '.join(tup) for tup in tuples)
            print "\nExpected:"
            print '\n'.join('    ' + ' '.join(tup) for tup in result_tuples)
            print

        return tuples == result_tuples

    def filter_by_query(quads, query, non_matching=False):
        result = []

        for e, p, v, g in quads:
            matches = ('e' not in query or e == query['e']) and \
                      ('p' not in query or p == query['p']) and \
                      ('v' not in query or v == query['v']) and \
                      ('g' not in query or g == query['g'])

            if matches ^ non_matching:
                result.append([e, p, v, g])

        return result

    def test_delete(**kwargs):
        deleted_quads = server.query(**kwargs)

        server.delete(**kwargs)

        assert same([quad for quad in server.query_all_graphs(1000) if quad[3] in TEST_GRAPHS],
                    filter_by_query(TEST_QUADS, kwargs, True))

        for e, p, v, g in deleted_quads:
            server.create(e, p, v, g)

    def cleanup(expected=True):
        if any(server.graph_exists(g) for g in TEST_GRAPHS) and not expected:
            print "Performing cleanup of previous test..."

        for g in TEST_GRAPHS:
            if server.graph_exists(g):
                server.delete_graph(g, True)

    # Start of tests
    server = GlobalServerInterface('http://localhost:8888/')
    #server = GlobalServerInterface('http://cassandra2-ersdevs.rhcloud.com/')

    # Check graph exists
    assert server.graph_exists('ers:bogusGraph1234') is False

    # Do cleanup of previous tests
    cleanup(False)

    # Create graph (add graph info)
    server.create_empty_graph('ers:testGraph1')
    server.create_empty_graph('ers:testGraph2')
    server.create_empty_graph('ers:testGraph3')
    assert server.graph_exists('ers:testGraph1') is True
    assert server.graph_exists('ers:testGraph2') is True
    assert server.graph_exists('ers:testGraph3') is True

    # Delete graph
    server.delete_graph('ers:testGraph3')
    assert server.graph_exists('ers:testGraph3') is False

    # Create
    for quad in TEST_QUADS:
        server.create(*quad)

    # Read entire graph
    assert same(server.query_graph('ers:testGraph1'),
                [quad for quad in TEST_QUADS if quad[3] == 'ers:testGraph1'])

    # Query e???
    assert same(server.query(e='ers:testEntity1'),
                [quad for quad in TEST_QUADS if quad[0] == 'ers:testEntity1'])

    # Query non-existing
    assert same(server.query(e='ers:bogusEntity5432'),
                [])

    # Query ?p??
    assert same(server.query(p='ers:testProp3'),
                [quad for quad in TEST_QUADS if quad[1] == 'ers:testProp3'])

    # Query ??v?
    assert same(server.query(e='ers:testValue1'),
                [quad for quad in TEST_QUADS if quad[2] == 'ers:testValue1'])

    # Query ep??
    assert same(server.query(e='ers:testEntity2', p='ers:testProp3'),
                [quad for quad in TEST_QUADS if quad[0] == 'ers:testEntity2' and quad[1] == 'ers:testProp3'])

    # Query e??g
    assert same(server.query(e='ers:testEntity1', g='ers:testGraph2'),
                [quad for quad in TEST_QUADS if quad[0] == 'ers:testEntity1' and quad[3] == 'ers:testGraph2'])

    # Query all graphs
    assert same([quad for quad in server.query_all_graphs(1000) if quad[3] in TEST_GRAPHS],
                TEST_QUADS)

    # Exist epvg (T)
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue31', g='ers:testGraph2') is True

    # Exist epvg (F)
    assert server.exist(e='ers:bogusEntity100', p='ers:testProp3', v='ers:testValue31', g='ers:testGraph2') is False

    # Exist epv? (T)
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue31') is True

    # Exist e??? (T)
    assert server.exist(e='ers:testEntity1') is True

    # Exist e??? (F)
    assert server.exist(e='ers:bogusEntity200') is False

    # Exist e??g (F)
    assert server.exist(e='ers:testEntity4', g='ers:testGraph1') is False

    # Delete epvg
    test_delete(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue31', g='ers:testGraph2')

    # Delete epv?
    test_delete(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue31')

    # Delete ep??
    test_delete(e='ers:testEntity1', p='ers:testProp3')

    # Delete e???
    test_delete(e='ers:testEntity1')

    # Delete e??g
    test_delete(e='ers:testEntity1', g='ers:testGraph2')

    # Delete non-existent e???
    test_delete(e='ers:bogusEntity1234')

    # Update existing value
    server.update('ers:testEntity1', 'ers:testProp3', 'ers:testValue31', 'ers:testValueNew', 'ers:testGraph1')
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue31', g='ers:testGraph1') is False
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValueNew', g='ers:testGraph1') is True

    # Update non-existing value
    server.update('ers:testEntity1', 'ers:testProp5', 'ers:testValue6', 'ers:testValue7', 'ers:testGraph1')
    assert server.exist(e='ers:testEntity1', p='ers:testProp5', v='ers:testValue6', g='ers:testGraph1') is False
    assert server.exist(e='ers:testEntity1', p='ers:testProp5', v='ers:testValue7', g='ers:testGraph1') is True

    # Update existing value with collision
    server.update('ers:testEntity1', 'ers:testProp3', 'ers:testValueNew', 'ers:testValue32', 'ers:testGraph1')
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValueNew', g='ers:testGraph1') is False
    assert server.exist(e='ers:testEntity1', p='ers:testProp3', v='ers:testValue32', g='ers:testGraph1') is True

    # Bulk load
    test_file = '../../tests/data/timbl.nt'
    bulk_quads = [[decode_rdflib_term(x) for x in tup] + ['ers:testGraphBulk']
                  for tup in rdflib.Graph().parse(test_file, format='nt')]

    server.create_empty_graph('ers:testGraphBulk')
    server.bulk_load('ers:testGraphBulk', file=test_file)

    assert same(server.query_graph('ers:testGraphBulk'),
                bulk_quads)

    # Cleanup
    cleanup()

    print "Tests pass"
    return


if __name__ == '__main__':
    test()
