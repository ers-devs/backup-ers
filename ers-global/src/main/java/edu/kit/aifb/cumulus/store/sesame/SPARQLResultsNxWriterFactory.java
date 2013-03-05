package edu.kit.aifb.cumulus.store.sesame;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;

/**
 * factory
 * 
 * @author aharth
 *
 */
public class SPARQLResultsNxWriterFactory implements TupleQueryResultWriterFactory {
	public static final TupleQueryResultFormat NX = new TupleQueryResultFormat("NX",
            Arrays.asList("text/nx"), Charset.forName("utf-8"),
            Arrays.asList("nx"));
	
    public TupleQueryResultFormat getTupleQueryResultFormat() {
        return NX;
    }

    /**
     * Returns a new instance of {@link SPARQLResultsXMLWriter}.
     */
    public TupleQueryResultWriter getWriter(OutputStream out) {
        return new SPARQLResultsNxWriter(out);
    }
}