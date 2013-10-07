package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class NTriplesFormat implements SerializationFormat {

	@Override
	public String getContentType() {
		return "text/plain";
	}

        @Override
        public int print(Iterator<Node[]> it, Writer pw, String author) throws IOException {
            return print(it, pw, author, -1);
        }

	@Override
	public int print(Iterator<Node[]> it, Writer pw, String author, int cutSuffixPos) throws IOException {
		int triples = 0;
		while (it.hasNext()) {
			Node[] nx = it.next();
			if (nx[0] != null && nx[1] != null && nx[2] != null) { // don't ask
                            for( int i=0; i<nx.length; ++i)
                                    pw.write(nx[i].toN3()+ " ");
                            pw.write(author + "\n");
                            triples++;
			}
		}
		return triples;
	}

	@Override
	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException {
		return new NxParser(is);
	}

}
