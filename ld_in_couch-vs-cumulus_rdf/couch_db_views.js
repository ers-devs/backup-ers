// by subject
function(doc) { 
        if(doc.doc_type == "RDFEntity" && doc.s) { 
                emit(doc.s+doc.g, doc);
        }   
}



//by object
function(doc) { 
    if(doc.doc_type == "RDFEntity" && doc.s) { 
		 if( doc.o.length > 0 ) {
			for( var i=0; i<doc.o.length; ++i) {
	 	       emit(doc.o[i], {_id:doc.o[i], s:doc.s[i], p:doc.p[i], o:doc.o[i], o_in:doc.o_in, doc_type:doc.doc_type});
			}
		}
    }   
}

// by object v2 , but not good as it keeps the whole doc as value 
function(doc) { 
    if(doc.doc_type == "RDFEntity" && doc.s) { 
		 if( doc.o.length > 0 ) {
			for( var i=0; i<doc.o.length; ++i) {
							if( doc.o[i].substr(0,7) == "http://" ) { 
					emit(doc.o[i], {p:doc.p[i]});
				}
	 	       emit(doc.o[i],doc);
			}
		}
    }   
}

// a trial of reduce function 
function( key, values, rereduce) 
{
	if( rereduce == false ) { 
		var res = [];
		for( var i=0; i<values.length; ++i ) {
			res.push(values[i].s);
		}
		return res.join();
	}
}	


// by object (without any value as the ids is enough) 
// TODO: reduce all documents with same key to only one having an array of ids or subjects 
// DID: but ... very slow with reduce ... hmmm, so KEEP IT AS IT IS HERE ! 
function(doc) { 
    if(doc.doc_type == "RDFEntity" && doc.s) { 
		 if( doc.o.length > 0 ) {
			for( var i=0; i<doc.o.length; ++i) {
				if( doc.o[i].substr(0,7) == "http://" || doc.o[i].substr(0,8) == "https://" ) { 
	 	       		emit(doc.o[i], {p:doc.p[i]});
				}
			}
		}
    }   
}

Reduce example: http://www.bitsbythepound.com/writing-a-reduce-function-in-couchdb-370.html
