## DER_verisign
============

Downscaling Entity Registries Project (with VeriSign)

## Purpose
===========
    - evaluating LD-in-Couch as RDF triples store based on CouchDB 
    - optimize LD-in-Couch for faster bulk loading 
    - this optimized version would play the role of data store for the local entity registry


## Installation
============
1.CouchDB: 
- it can be done either by compiling the sources or just running an 'apt-get install couchdb' on Debian distros 
- for futher information about installing CouchDB, please check this: http://wiki.apache.org/couchdb/Installatio	n
- we used CouchDB version 1.2.0 

2.Couchdbkit (a framwork for allowing Python application to use CouchDB):
- check its website for futher information: http://couchdbkit.org/download.html

3.LD-in-Couch (optional):    
- if you would like to get the basic version of LD-in-Couch, it can be downloaded from: https://github.com/mhausenblas/ld-in-couch
- this is optional, as the optimized version is based on the basic one
    
## Configurations
=============
Check the following parameters from the header of the file 2_optimized_ld-in-couch.py:
- COUCHDB_SERVER should point to the URL of the running couchdb 
- COUCHDB_USERNAME / COUCHDB_PASSWORD should contain login information of an user having admin access (for creating the DB)
		- to create an user you should create a document under _users table (see http://stackoverflow.com/questions/3684749/creating-regular-users-in-couchdb)
		- for enabling it as admin, one should modify ${COUCHDB_INSTALL_DIR}/etc/couchdb/local.ini and add the just created user in [admin] section 
		- however, to bypass this step, one could easily interchange the 2 code lines under the "# auth is bypassed here"
- BULK_LOAD_DOCS is the number of documents cached before a flush on disk is fired; we experimented and 10k was the best on our system; however, it may change on other system 
- PATH_TO_DESIGN_DOCS points to a directory containing the .js files for creating by_object and by_predicate views; by default it looks under the current directory; modify this accordingly

## Usage
=========
Loading an RDF .nt file. The file must be white-spaces separed values and for better performance it has to be sorted by subject. The command creats the DB, the views and compacts them: 

	$: python 2_optimized_ld-in-couch.py -i mdb-sorted-fixed.nt -g optimized_linkedmdb -d optimized_linkedmdb
	Parameters: 
	1. -i <input_file> 
	2. -g <RDF_target_graph>
	3. -d <DB_name>
	

Query by subject example:

	curl -X GET ’http://localhost:5984/replicate_rdf1mil/_design/entity/_view/by_subject?key="http%3A//www.owl-ontologies.com/Ontology1347094758.owlrepo1mil_test"’
	
Query by object example: 

	curl -X GET ’http://localhost:5984/optimized2_1mil/_design/entity/_view/by_object?key="http%3A//www.w3.org/2002/07/owl%23Ontology"’
	
Query by predicate example:

	curl -X GET ’http://localhost:5984/optimized2_1mil/_design/entity/_view/by_predicate?key="http%3A//www.unifr.ch/philosophy.owl%23beginsOnDate"’

## License and Acknowledgements

This software is licensed under Apache 2.0 Software License.
	

