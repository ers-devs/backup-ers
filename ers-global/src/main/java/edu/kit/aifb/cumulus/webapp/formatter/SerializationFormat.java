package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.ParseException;

public interface SerializationFormat {
	public String getContentType();
	
	public int print(Iterator<Node[]> it, Writer pw, String author, boolean cutSuffix) throws IOException;

        public int print(Iterator<Node[]> it, Writer pw, String author) throws IOException;

	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException;
}
