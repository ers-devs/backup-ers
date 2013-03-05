package edu.kit.aifb.cumulus.store.sesame;

/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.rio.turtle.TurtleWriter;

/**
 * An implementation of the TupleQueryresultWriter interface that writes Nx bindings.
 * (similar to CSV, but with N3/Turtle syntax for nodes).
 * 
 * @author aharth
 */
public class SPARQLResultsNxWriter extends TurtleWriter implements TupleQueryResultWriter {
    /*--------------*
     * Constructors *
     *--------------*/

    /**
     * Creates a new TurtleWriter that will write to the supplied OutputStream.
     * 
     * @param out
     *        The OutputStream to write the Turtle document to.
     */
    public SPARQLResultsNxWriter(OutputStream out) {
        this(new OutputStreamWriter(out, Charset.forName("UTF-8")));
    }

    /**
     * Creates a new TurtleWriter that will write to the supplied Writer.
     * 
     * @param writer
     *        The Writer to write the Turtle document to.
     */
    public SPARQLResultsNxWriter(Writer writer) {
    	super(writer);
    }

    /*---------*
     * Methods *
     *---------*/

    void EOL() throws IOException {
		writer.write(".");
		writer.writeEOL();
    }
    
	@Override
	public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
		try {
			for (Binding b : bs) {
				writeValue(b.getValue());
				writer.write(" ");
			}
			EOL();
		} catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	@Override
	public void startQueryResult(List<String> vars) throws TupleQueryResultHandlerException {
        if (writingStarted) {
            throw new RuntimeException("Document writing has already started");
        }

        writingStarted = true;

		try {
			for (String var : vars) {
				writer.write("?");
				writer.write(var);
				writer.write(" ");
			}
			EOL();
		}
		catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	@Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
		if (!writingStarted) {
			throw new RuntimeException("Document writing has not yet started");
		}

		try {
			writer.flush();
		}
		catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
		finally {
			writingStarted = false;
		}
	}

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return SPARQLResultsNxWriterFactory.NX;
	}
}