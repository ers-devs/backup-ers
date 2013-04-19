package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.ParseException;

public interface SerializationFormat {
	public String getContentType();
	
	public int print(Iterator<Node[]> it, PrintWriter pw, String author);

	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException;
}
