package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

public class SimpleRDFXMLFormat implements SerializationFormat {

	@Override
	public String getContentType() {
		return "application/rdf+xml";
	}
	
	@Override
	public int print(Iterator<Node[]> it, PrintWriter out, String author) {
		out.println("<?xml version='1.0'?>");
		out.println("<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'>");

		int triples = 0;
		Node oldsubj = null;
		List<Node[]> list = new ArrayList<Node[]>();
		while (it.hasNext()) {
			Node[] nx = it.next();
			Node subj = nx[0];
			if (nx[0] == null || nx[1] == null || nx[2] == null) // don't ask
				continue;

			triples++;
			
			// new subject encountered
			if (oldsubj != null && !subj.equals(oldsubj)) {
				printRDFXML(list, out);
				list = new ArrayList<Node[]>();
			}

			list.add(nx);

			oldsubj = subj;

		}

		if (!list.isEmpty()) {
			printRDFXML(list, out);
		}

		out.println("</rdf:RDF>");
		
		return triples;
	}

	private void printRDFXML(List<Node[]> list, PrintWriter out) {
		if (list.isEmpty()) {
			return;
		}

		Node subj = list.get(0)[0];
		out.print("<rdf:Description");

		if (subj instanceof Resource) {
			out.println(" rdf:about='" + escape(subj.toString()) + "'>");
		} else if (subj instanceof BNode) {
			out.println(" rdf:nodeID='" + subj.toString() + "'>");
		}

		for (Node[] ns: list) {
			String r = ns[1].toString();
			String namespace = null, localname = null;
			int i = r.indexOf('#');

			if (i > 0) {
				namespace = r.substring(0, i+1);
				localname = r.substring(i+1, r.length());
			} else {
				i = r.lastIndexOf('/');
				if (i > 0) {
					namespace = r.substring(0, i+1);
					localname = r.substring(i+1, r.length());
				}
			}
			if (namespace == null && localname == null) {
				System.err.println("couldn't separate namespace and localname");
				break;
			}

			out.print("\t<" + localname + " xmlns='" + namespace + "'");
			if (ns[2] instanceof BNode) {
				out.println(" rdf:nodeID='" + ns[2].toString() + "'/>");
			} else if (ns[2] instanceof Resource) {
				out.println(" rdf:resource='" + escape(ns[2].toString()) + "'/>");				
			} else if (ns[2] instanceof Literal) {
				Literal l = (Literal)ns[2];
				if (l.getLanguageTag() != null) {
					out.print(" xml:lang='" + l.getLanguageTag() + "'");
				} else if (l.getDatatype() != null) {
					out.print(" rdf:datatype='" + l.getDatatype().toString() + "'");					
				}
				out.println(">" + escape(ns[2].toString()) + "</" + localname + ">");
			}
		}

		out.println("</rdf:Description>");
	}

	private String escape(String s){
		String e;
		e = s.replaceAll("&", "&amp;");
		e = e.replaceAll("<", "&lt;");
		e = e.replaceAll(">", "&gt;");
		e = e.replaceAll("\"","&quot;");
		e = e.replaceAll("'","&apos;");
		e = e.replaceAll(" ", "+");
		return e;
	}

	@Override
	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException {
		return new RDFXMLParser(is, "http://example.org/");
	}

}
