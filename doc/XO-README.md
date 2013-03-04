XO README
=========

## CouchDB

### Installation 


Try installing using
    yum install couchdb

This will probably fail due to insuficient memory. There are 2 things to try before attempting the installation again:

* Turn on swapping as described here: <http://wiki.laptop.org/go/Swap>
* Run yum from console (Ctrl-Alt-F2)

### Configuration

CouchDB configuration file is located in `couch/local.ini`

### Running

Use `couch/couchstart` to start CouchDB. No preveleges are required to start the server as all the files are kept in `couch/couchdata`. To stop CouchDB server use `couch/couchstop`. To start with a clean database just delete `couch/couchdata`.

## Python

LD-in-couch and its optimised versions rely on couchdbkit for communication with CouchDB. CouchDB Kit and its dependencies are located in `site-packages`. You may copy them to python's site-packages directory or run

    export PYTHONPATH=/Full/path/to/the/repository/site-packages:$PYTHONPATH

before using the scripts.

## XO's

* SSH access is enabled on both XO's (see <http://wiki.laptop.org/go/Ssh_into_the_XO>)
* root password is set to "wikireg".
* SemanticXO is still running. If you can't run activities try to clean the redstore data directory. 

##Test Protocol

1. Reboot XO
2. Open console
3. Start couchdb with a clean data directory
4. Run tests
