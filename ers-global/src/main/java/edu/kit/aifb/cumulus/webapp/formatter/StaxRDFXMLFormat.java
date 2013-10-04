package edu.kit.aifb.cumulus.webapp.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

public class StaxRDFXMLFormat implements SerializationFormat {

	@Override
	public String getContentType() {
		return "application/rdf+xml";
	}

        @Override
        public int print(Iterator<Node[]> it, Writer pw, String author) throws IOException  {
            return print(it, pw, author, -1);
        }

	@Override
	public int print(Iterator<Node[]> it, Writer pw, String author, int cutSuffixesPos) throws IOException  {
		XMLOutputFactory factory = XMLOutputFactory.newInstance();
		factory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, Boolean.TRUE);

		int triples = 0;
		try {
			XMLStreamWriter ch = factory.createXMLStreamWriter(pw);

			ch.writeStartDocument("utf-8", "1.0");
	
			ch.writeStartElement("rdf:RDF");
			ch.writeNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
	
			Node oldsubj = null;
			List<Node[]> list = new ArrayList<Node[]>();
			while (it.hasNext()) {
				Node[] nx = it.next();
				Node subj = nx[0];
	
				triples++;
				
				// new subject encountered
				if (oldsubj != null && !subj.equals(oldsubj)) {
					printRDFXML(list, ch);
					list = new ArrayList<Node[]>();
				}
				list.add(nx);
				oldsubj = subj;
			}
	
			if (!list.isEmpty()) {
				printRDFXML(list, ch);
			}
	
			pw.write("</rdf:RDF>");
		}
		catch (XMLStreamException e) {
			e.printStackTrace();
		}
		
		return triples;
	}

	private static void printRDFXML(List<Node[]> list, XMLStreamWriter ch) throws XMLStreamException {
		if (list.isEmpty()) {
			return;
		}

		Node subj = list.get(0)[0];

		ch.writeStartElement("rdf:Description");

		if (subj instanceof Resource) {
			ch.writeAttribute("rdf:about", subj.toString());
		} else if (subj instanceof BNode) {
			ch.writeAttribute("rdf:nodeID", subj.toString());
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
			if (namespace == null || localname == null) {
				System.err.println(ns[1].toString());
				System.err.println("couldn't separate namespace and localname");
				break;
			}

			ch.writeStartElement(namespace, localname);
			if (ns[2] instanceof BNode) {
				ch.writeAttribute("rdf:nodeID", ns[2].toString());
			} else if (ns[2] instanceof Resource) {
				ch.writeAttribute("rdf:resource", ns[2].toString());
			} else if (ns[2] instanceof Literal) {
				Literal l = (Literal)ns[2];
				if (l.getLanguageTag() != null) {
					ch.writeAttribute("xml:lang", l.getLanguageTag());
				} else if (l.getDatatype() != null) {
					ch.writeAttribute("rdf:datatype", l.getDatatype().toString());
				}
				ch.writeCharacters(l.toString());
			}
			ch.writeEndElement();
		}
		
		ch.writeEndElement();
	}
	
	@Override
	public Iterator<Node[]> parse(InputStream is) throws ParseException, IOException {
		return new RDFXMLParser(is, "http://example.org/");
	}
}
