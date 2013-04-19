package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.util.NxUtil;

public class HTMLFormat implements SerializationFormat {

	@Override
	public String getContentType() {
		return "text/html";
	}

	@Override
	public int print(Iterator<Node[]> it, PrintWriter pw, String author) {
		pw.println("<html><head></head><body>");
		
		int triples = 0;
		while (it.hasNext()) {
			Node[] nx = it.next();
			if (nx[0] != null && nx[1] != null && nx[2] != null) { // don't ask
				pw.println(NxUtil.escapeForMarkup(Nodes.toN3(nx))+ " " + author);
				pw.println("<br/>");
				triples++;
			}
		}
		
		pw.println("</body></html>");
		
		return triples;
	}

	@Override
	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException {
		return null;
	}
}
