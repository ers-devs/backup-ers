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

    def delete_graph(self, graph, force=False):
        self._do_request('graph', {'g': graph, 'f': 'y' if force else 'n'}, 'DELETE')

    def create(self, entity, prop, value, graph):
        self._do_request('create', {'e': entity, 'p': prop, 'v': value, 'g': graph}, 'POST')

    def query_graph(self, graph):
        return self._do_quads_request('query_graph', {'g': graph})

    def query(self, **kwargs):
        params = {}
        for key in ['e', 'p', 'v', 'g']:
            if key in kwargs:
                params[key] = kwargs[key]

        try:
            tuples = self._do_quads_request('query', params)

            return tuples
        except GlobalServerOperationException as e:
            if 'resource not found' in str(e):
                return []
            else:
                raise e

    def delete(self, entity, prop, value):
        self._do_write_request('delete', {'e': entity, 'p': prop, 'v': value})

    def update(self, entity, prop, old_value, new_value):
        self._do_write_request('update', {'e': entity, 'p': prop, 'v_old': old_value, 'v_new': new_value})

    def bulk_load(self, **kwargs):
        data = None
        if 'file' in kwargs:
            with open(kwargs['file'], 'r') as f:
                data = f.read()
        elif 'tuples' in kwargs:
            data = ''.join(' '.join(encode_rdflib_term(x).n3() for x in tup) + '.\n'
                           for tup in kwargs['tuples'])
        elif 'data' in kwargs:
            data = kwargs['data']

        if data is None:
            raise GlobalServerOperationException("You must specify file=, data= or tuples= with bulk_load()")

        boundary = mimetools.choose_boundary()

        req_url = self._server_url + 'bulkload'

        req_data = '\r\n'.join([
            '--' + boundary,
            'Content-Disposition: form-data; name="filedata"; filename="tuples.nt"',
            'Content-Type: application/octet-stream',
            '',
            data,
            '--' + boundary + '--',
            ''
        ])

        request = urllib2.Request(req_url, req_data)
        request.add_header('Accept', 'text/html')
        request.add_header('Content-Type', 'multipart/form-data; boundary=' + boundary)
        request.add_header('Content-Length', len(req_data))

        self._do_request(request)

    def _do_request(self, operation, params, method='GET'):
        req_url = self._server_url + urllib.quote_plus(operation)
        req_data = None

        if method=='POST':
            req_data = self._encode_params(params)
        else:
            req_url = req_url + '?' + self._encode_params(params)

        request = RequestEx(req_url, req_data, method=method)
        request.add_header('Accept', 'text/html')

        try:
            response = urllib2.urlopen(request, None, self.timeout_sec)

            return response.read()
        except urllib2.HTTPError as e:
            err_body = e.read()

            match = re.match(r'<html><body><h1>Error</h1><p>Status code \d+</p><p>(.*)</p><p>(.*)</p>\s*</body><html>',
                             err_body, re.I | re.S)
            if match and match.group(1) != match.group(2):
                raise GlobalServerOperationException('Error in global server operation: ' + match.group(2))
            else:
                raise GlobalServerAccessException('Cannot access global server: ' + str(e))
        except urllib2.URLError as e:
            raise GlobalServerAccessException('Cannot access global server: ' + str(e))
        except Exception as e:
            raise GlobalServerAccessException('Cannot access global server: ' + str(e))

    # TODO: Eventually we should distinguish internally between literal URLs and URIRefs. Till then...
    def _encode_params(self, params):
        return urllib.urlencode([(k, encode_rdflib_term(v).n3()) for k, v in params.items()])

    def _do_bool_request(self, operation, params):
        response = self._do_request(operation, params)

        if response.startswith('TRUE'):
            return True
        elif response.startswith('FALSE'):
            return False
        else:
            raise GlobalServerInternalException(
                "Invalid response to {0}; expected TRUE or FALSE, got '{0}'".format(operation, response))

    def _temp_filter_html_response(self, response):
        if response.startswith('<html'):
            response = response.replace('<br/>', '\n')
            response = re.sub(r'<[^>]*>', '', response)
            response = html_entity_decode(response)

        return response

    def _do_quads_request(self, operation, params):
        PAT_N3_VALUE = r'(<[^>]+>|"[^"]*"[^ ]*|_[^ ]+)'
        PAT_QUADS_LINE = '{0} {0} {0} . {0}'.format(PAT_N3_VALUE)

        response = self._do_request(operation, params)

        # TODO: remove this when it is no longer necessary
        response = self._temp_filter_html_response(response)

        quads = []
        for line in response.split('\n'):
            if line == "":
                continue
            if line.startswith('Total quads returned:'):
                break

            match = re.match(PAT_QUADS_LINE, line, re.I)
            if match is None:
                raise GlobalServerInternalException("Malformed line when receving quads response:\n" + line)

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

    def read_all(entities):
        tuples = []
        for e in entities:
            result = server.read(e)
            if result is not None:
                tuples.extend(result)

        return tuples

    def cleanup(expected=True):
        graphs = ['ers:testGraph1', 'ers:testGraph2', 'ers:testGraph3', 'ers:testGraphBulk']

        if any(server.graph_exists(g) for g in graphs) and not expected:
            print "Performing cleanup of previous test..."

        for g in graphs:
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
    server.add_graph_info('ers:testGraph1', URN_RDF_TYPE, URN_RDFG_GRAPH)
    server.add_graph_info('ers:testGraph2', URN_RDF_TYPE, URN_RDFG_GRAPH)
    server.add_graph_info('ers:testGraph3', URN_RDF_TYPE, URN_RDFG_GRAPH)
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

    # Cleanup
    cleanup()

    print "Tests OK so far"
    return



    test_file = '../../tests/data/timbl.nt'
    bulk_tuples = [[decode_rdflib_term(x) for x in tup]
                   for tup in rdflib.Graph().parse(test_file, format='nt')]



    # Delete existent
    server.delete('ers:testEntity1', 'ers:testProp2', 'ers:testValue2')
    assert same(server.read('ers:testEntity1'),
                [['ers:testEntity1', 'ers:testProp1', 'testValue1'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue31'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32']])

    # Delete non-existent
    server.delete('ers:testEntity1', 'ers:testProp3', 'ers:testValueZZ')
    assert same(server.read('ers:testEntity1'),
                [['ers:testEntity1', 'ers:testProp1', 'testValue1'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue31'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32']])

    # Update existent triple
    server.update('ers:testEntity1', 'ers:testProp3', 'ers:testValue31', 'ers:testValueXX')
    assert same(server.read('ers:testEntity1'),
                [['ers:testEntity1', 'ers:testProp1', 'testValue1'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValueXX']])

    # Update non-existent triple
    server.update('ers:testEntity1', 'ers:testProp5', 'ers:testValue6', 'ers:testValue7')
    assert same(server.read('ers:testEntity1'),
                [['ers:testEntity1', 'ers:testProp1', 'testValue1'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValueXX'],
                 ['ers:testEntity1', 'ers:testProp5', 'ers:testValue7']])

    # Update existent triple w/ collision
    server.update('ers:testEntity1','ers:testProp3', 'ers:testValueXX', 'ers:testValue32')
    assert same(server.read('ers:testEntity1'),
                [['ers:testEntity1', 'ers:testProp1', 'testValue1'],
                 ['ers:testEntity1', 'ers:testProp3', 'ers:testValue32'],
                 ['ers:testEntity1', 'ers:testProp5', 'ers:testValue7']])

    # Bulk load
    bulk_entities = set(e for e, _, _ in bulk_tuples)

    server.bulk_load(file='../../tests/data/timbl.nt')
    assert same(read_all(bulk_entities), bulk_tuples)

    # Cleanup
    cleanup()

    print "Tests pass"
    return


if __name__ == '__main__':
    test()
