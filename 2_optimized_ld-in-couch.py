#!/usr/bin/python

# 
# TEODOR, 1st version: try here to use bulk loading and to make it faster !
# TEODOR, 2nd version: try now to insert smarter and keep one document per subject (multiples predicate,object values)
#

""" 
  Enables you to store, process and query RDF-based Linked Data in Apache CouchDB.
	!!! THIS CREATES AT THE END ALL THE VIEWS, SO NOT INCREMENTALLY AS v3
		NO REDUCE FUNCTION ARE USED AS THEY ARE INCREDIBLE SLOW ... SO JUST 
		CREATE ONE DOCUMENT FOR EACH TRIPLE FOR THE VIEWS

@author1: Michael Hausenblas, http://mhausenblas.info/#i
@author2: Teodor Macicas 
@since1: 2012-10-06
@since2: 2012-11-27
@status: init
"""

import os
import sys
import logging
import getopt
import string
import StringIO
import urlparse
import urllib
import urllib2
import string
import cgi
import time
import datetime
import json
import io
import md5
from BaseHTTPServer import BaseHTTPRequestHandler
from os import curdir, sep
from couchdbkit import Server, Database, Document, StringProperty, DateTimeProperty, StringListProperty, BulkSaveError
from restkit import BasicAuth, set_logging
from couchdbkit.loaders import FileSystemDocsLoader

# Configuration, change as you see fit
#DEBUG = True
DEBUG = False
# not quite used :)  
PORT = 7172

COUCHDB_SERVER = 'http://127.0.0.1:5984/'
COUCHDB_USERNAME = 'admin'
COUCHDB_PASSWORD = 'admin'
# cache BULK_LOAD_DOCS and then bulk load all of them once :) 
BULK_LOAD_DOCS = 10000
PATH_TO_DESIGN_DOCS = './_design/'
#PATH_TO_DESIGN_DOCS = '/home/teodor/couchdb/_design/'

# way slower if this is activated, so we prefer creating views instead of this
ADD_BACK_LINKS = False

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')

# The main LD-in-Couch service
class LDInCouchServer(BaseHTTPRequestHandler):

	# changes the default behavour of logging everything - only in DEBUG mode
	def log_message(self, format, *args):
		if DEBUG:
			try:
				BaseHTTPRequestHandler.log_message(self, format, *args)
			except IOError:
				pass
		else:
			return
	
	# reacts to GET request by serving static content in standalone mode as well as
	# handles API calls for managing content
	def do_GET(self):
		parsed_path = urlparse.urlparse(self.path)
		target_url = parsed_path.path[1:]
		
		# API calls
		if self.path.startswith('/q/'):
			self.send_error(404,'File Not Found: %s' % self.path) #self.serve_lookup(self.path.split('/')[-1])
		# static stuff (for standalone mode - typically served by Apache or nginx)
		elif self.path == '/':
			self.serve_content('index.html')
		elif self.path.endswith('.ico'):
			self.serve_content(target_url, media_type='image/x-icon')
		elif self.path.endswith('.html'):
			self.serve_content(target_url, media_type='text/html')
		elif self.path.endswith('.js'):
			self.serve_content(target_url, media_type='application/javascript')
		elif self.path.endswith('.css'):
			self.serve_content(target_url, media_type='text/css')
		elif self.path.startswith('/img/'):
			if self.path.endswith('.gif'):
				self.serve_content(target_url, media_type='image/gif')
			elif self.path.endswith('.png'):
				self.serve_content(target_url, media_type='image/png')
			else:
				self.send_error(404,'File Not Found: %s' % target_url)
		else:
			self.send_error(404,'File Not Found: %s' % target_url)
		return
	
	# look up an entity
	def serve_lookup(self, entryid):
		pass
		# try:
		# 	backend = 
		# 	(entry_found, entry) = backend.find(entryid)
		# 	
		# 	if entry_found:
		# 		self.send_response(200)
		# 		self.send_header('Content-type', 'application/json')
		# 		self.end_headers()
		# 		self.wfile.write(json.dumps(entry))
		# 	else:
		# 		self.send_error(404,'Entry with ID %s not found.' %entryid)
		# 	return
		# except IOError:
		# 	self.send_error(404,'Entry with ID %s not found.' %entryid)	
	
	# serves static content from file system
	def serve_content(self, p, media_type='text/html'):
		try:
			f = open(curdir + sep + p)
			self.send_response(200)
			self.send_header('Content-type', media_type)
			self.end_headers()
			self.wfile.write(f.read())
			f.close()
			return
		except IOError:
			self.send_error(404,'File Not Found: %s' % self.path)
	
	# serves remote content via forwarding the request
	def serve_URL(self, remote_url, media_type='application/json'):
		logging.debug('REMOTE GET %s' %remote_url)
		self.send_response(200)
		self.send_header('Content-type', media_type)
		self.end_headers()
		data = urllib.urlopen(remote_url)
		self.wfile.write(data.read())
	

# A single entity, expressed in RDF data model
class RDFEntity(Document):
	g = StringProperty() # the graph this entity belongs to
	s = StringProperty() # the one and only subject
	p = StringListProperty() # list of predicates
	o = StringListProperty() # list of objects
	if ADD_BACK_LINKS:
		o_in = StringListProperty() # list of back-links (read: 'object in')

# The Apache CouchDB backend for LD-in-Couch
class LDInCouchBinBackend(object):
	
	# init with URL of CouchDB server, database name, and credentials
	def __init__(self, serverURL, dbname, username, pwd):
		self.serverURL = serverURL
		self.dbname = dbname
		self.username = username
		self.pwd = pwd

		# auth is bypassed here (by TEODOR)
		# self.server = Server(self.serverURL)
		self.server = Server(self.serverURL, filters=[BasicAuth(self.username, self.pwd)])
		set_logging('info') # suppress DEBUG output of the couchdbkit/restkit
	
	# looks up a document via its ID 
	def look_up_by_id(self, eid):
		try:
			db = self.server.get_or_create_db(self.dbname)
			if db.doc_exist(eid):
				ret = db.get(eid)
				return (True, ret)
			else:
				return (False, None)
		except Exception as err:
			logging.error('Error while looking up entity: %s' %err)
			logging.error(eid)
			return (False, False)
	
	# finds an RDFEntity document by subject and returns its ID, for example:
	# curl 'http://127.0.0.1:5984/rdf/_design/lookup/_view/by_subject?key="http%3A//example.org/%23r"'
	def look_up_by_subject(self, subject, in_graph):
		viewURL = ''.join([COUCHDB_SERVER, LOOKUP_BY_SUBJECT_PATH, '"', urllib.quote(subject), urllib.quote(in_graph), '"'])
		logging.debug(' ... querying view %s ' %(viewURL))
		doc = urllib.urlopen(viewURL)
		doc = json.JSONDecoder().decode(doc.read())
		if len(doc['rows']) > 0:
			eid = doc['rows'][0]['id']
			logging.debug('Entity with %s in subject position (in graph %s) has the ID %s' %(subject, in_graph, eid))
			return eid
		else:
			logging.debug('Entity with %s in subject position does not exist, yet in graph %s' %(subject, in_graph))
			return None
	
	
	# imports an RDF NTriples file triple by triple into JSON documents of RDFEntity type
	# as of the pseudo-algorthim laid out in https://github.com/mhausenblas/ld-in-couch/blob/master/README.md
	def import_NTriples(self, file_name, target_graph):
		triple_count = 0
		subjects = [] # for remembering which subjects we've already seen
		logging.info('Starting import ...')
		# get current time 
		start = time.time()
		input_doc = open(file_name, "r")
		db = self.server.get_or_create_db(self.dbname)
		RDFEntity.set_db(db) # associate the document type with database

		# save here documents in order to bulk load !! 
		doc_cache = dict() 
		# keep a hashtable of already loaded subjects and their (rev,id) values
		subj_ht = dict() 
		# flag for signaling the first bulk loading
		first_flush = 0
		
		if(not target_graph):
			target_graph = file_name
		
		logging.info('Importing NTriples file \'%s\' into graph <%s>' %(file_name, target_graph))
		back_links = 0

		# scan each line (triple) of the input document
		for input_line in input_doc:
			 # parsing a triple @@FIXME: employ real NTriples parser here!
			triple = input_line.split(' ', 2) # naively assumes SPO is separated by a single whitespace
			is_literal_object = False
			s = triple[0][1:-1] # get rid of the <>, naively assumes no bNodes for now
			# append the target graph as subject 
			s = s + "#" + target_graph
			p = triple[1][1:-1] # get rid of the <>
			o = triple[2][1:-1] # get rid of the <> or "", naively assumes no bNodes for now
			oquote = triple[2][0]
			if oquote == '"':
				o = triple[2][1:].rsplit('"')[0]
				is_literal_object = True
			elif oquote == '<':
				o = triple[2][1:].rsplit('>')[0]
			else:
				o = triple[2].split(' ')[0] # might be a named node

			logging.debug('-'*20)
			logging.debug('#%d: S: %s P: %s O: %s' %(triple_count, s, p, o))

	
			# this may be quite slow for big datasets !! 
			if ADD_BACK_LINKS: 
				# md5.new(obj).hexadigest() -> in order to use hash if needed
				# THE ADD BACK LINKS LOGIC IS HERE 
				# add back links if the document is in cache 
				if not is_literal_object:
					# if document is not yet bulk loaded, then just add this 's' to its 'o_in'
					if o in doc_cache: 
						doc_from_cache = doc_cache.get(o)
						doc_from_cache.o_in.append(s)	
					else: 
						if first_flush is 1: 
							# check if object already appeared as subject
							if o in subj_ht: 	
								# START get doc from disk
								# then get the documents attributes from disk
								ret = self.look_up_by_id(o)
								back_links += 1
								while ret[0] is False and ret[1] is False:
									back_links += 1
									ret = self.look_up_by_id(o)
								# END get doc from disk	
								if ret[0] is True: 
									# there is already such a document, 
									# so get its _rev value and use it in order to update it
									doc = RDFEntity(_id=o, _rev=ret[1]['_rev'], g=target_graph, s=o,  p=[], o=[], o_in=[s]) 
									doc.p.extend(ret[1]['p'])
									doc.o.extend(ret[1]['o'])
									doc.o_in.extend(ret[1]['o_in'])
									doc_cache[o] = doc
							else:
								doc = RDFEntity(_id=o, g=target_graph, s=o, p=[], o=[], o_in=[s])
								doc_cache[o] = doc
								subj_ht[o] = 1
				# END BACK LINK STUFF 


			# check if a document with same subject exists in the doc_cache (the not-yet-bulk-loaded docs)
			if s in doc_cache:
				doc_from_cache = doc_cache.get(s) 
				# add here the new predicate + object
				doc_from_cache.p.append(p)
				doc_from_cache.o.append(o)
			else: 
				doc = RDFEntity(_id=s, g=target_graph, s=s,  p=[p], o=[o])
				# being in subj_ht means here that the document is already flushed
				if first_flush is 1 and s in subj_ht: 	
					# then get the documents attributes from disk
					ret = self.look_up_by_id(s)
					while ret[0] is False and ret[1] is False : 
						ret = self.look_up_by_id(s)
					if ret[0] is True: 
						# there is already such a document, 
						# so get it's _rev value and use it in order to update it
						doc = RDFEntity(_id=s, _rev=ret[1]['_rev'], g=target_graph, s=s,  p=[p], o=[o]) 
						doc.p.extend(ret[1]['p'])
						doc.o.extend(ret[1]['o'])
				# ... so create a new entity doc
				# add here in cache 
				doc_cache[s] = doc
				subj_ht[s] = 1
			
			triple_count += 1
		 	if len(doc_cache) >= BULK_LOAD_DOCS: 
				try:
					tmp = db.save_docs(doc_cache.values())
				except BulkSaveError as e:
					print e.errors 
#				logging.info(tmp)
				logging.info("ANOTHER %d triples have been saved ... ", BULK_LOAD_DOCS)
				# now empty the cache
				doc_cache.clear()
				if first_flush == 0: 
					first_flush = 1


		# save documents one more time here, maybe <BULK_LOAD_DOCS are not loaded
	 	if len(doc_cache) >= 0: 
			# save all documents here once ! (hope it's possible :) )		
			try:
				tmp = db.save_docs(doc_cache.values())
			except BulkSaveError as e : 
				print e.errors
			#logging.info(tmp)
			# now empty the cache
			doc_cache.clear()
		logging.info('Import completed. I\'ve processed %d triples and seen %d subjects.' %(triple_count, len(subj_ht)))

		end = time.time() 
		logging.info('Importing completed in %d seconds.' %(end-start))
		# now create the by_object view if back links were not created
		if not ADD_BACK_LINKS: 
			start = time.time() 
			# load the design docs 
			loader = FileSystemDocsLoader(PATH_TO_DESIGN_DOCS)
 			loader.sync(db, verbose=True)
			logging.info('Now let\'s create the by_object view.')
			# query once for constructing the DB
			res = db.view("entity/by_object")
			res.first()
			end = time.time() 
			logging.info('Done. It completed in %d seconds.' %(end-start))
			start = time.time() 
			logging.info('Now let\'s create the by_predicate view.')
			# query once for constructing the DB
			res = db.view("entity/by_predicate")
			res.first()
			end = time.time() 
			logging.info('Done. It completed in %d seconds.' %(end-start))
			# now compact the views 
			logging.info("Now start compacting the views ... ")
			start = time.time() 
			for design_doc_name in list(db.all_docs(startkey="_design", endkey="_design0", wrapper=lambda row: row['id'][len('_design/'):])):
				logging.info("Compacting design document %s..." % design_doc_name)	
				db.compact(design_doc_name) 
			end = time.time() 
			logging.info("Done compacting the views in %d seconds !" %(end-start))
	
def usage():
	# modified by TEODOR (added -d)
	print('Usage: python ld-in-couch.py -c $couchdbserverURL -u $couchdbUser -p $couchdbPwd -d $database')
	print('To import an RDF NTriples document (can specify target graph with -g if you want to):')
	print(' python ld-in-couch.py -i data/example_0.nt')
	print('To run the service (note: these are all defaults, so don\'t need to specify them):')
	print(' python ld-in-couch.py -c http://127.0.0.1:5984/ -u admin -p admin')

if __name__ == '__main__':
	do_import = False
	target_graph = ''
	try:
		# extract and validate options and their arguments
		logging.info('-'*80)
		logging.info('*** CONFIGURATION ***')
		opts, args = getopt.getopt(sys.argv[1:], 'hi:g:c:u:p:d:', ['help', 'import=', 'graph=', 'couchdbserver=', 'username=', 'password=', 'database='])
		for opt, arg in opts:
			if opt in ('-h', '--help'):
				usage()
				sys.exit()
			elif opt in ('-i', '--import'):
				input_file = os.path.abspath(arg)
				do_import = True
			elif opt in ('-g', '--graph'):
				target_graph = arg
			elif opt in ('-c', '--couchdbserver'):
				couchdbserver = arg
				logging.info('Using CouchDB server: %s' %couchdbserver)
			elif opt in ('-u', '--username'):
				couchdbusername = arg
				logging.info('Using CouchDB username: %s' %couchdbusername)
			elif opt in ('-p', '--password'): 
				couchdbpassword = arg
				logging.info('Using CouchDB password: %s' %couchdbpassword)
			# added by TEODOR
			elif opt in ('-d', '--database'):
				COUCHDB_DB = arg
				LOOKUP_BY_SUBJECT_PATH = arg + '/_design/entity/_view/by_subject?key='

		logging.info('-'*80)
		
		if do_import:
			backend = LDInCouchBinBackend(serverURL = COUCHDB_SERVER , dbname = COUCHDB_DB, username = COUCHDB_USERNAME, pwd = COUCHDB_PASSWORD)
			backend.import_NTriples(input_file, target_graph)
		else:
			from BaseHTTPServer import HTTPServer
			server = HTTPServer(('', PORT), LDInCouchServer)
			logging.info('LDInCouchServer started listening on port %s, use {Ctrl+C} to shut-down ...' %PORT)
			server.serve_forever()
	except getopt.GetoptError, err:
		print str(err)
		usage()
		sys.exit(2)
