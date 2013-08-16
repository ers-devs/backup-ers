package edu.kit.aifb.cumulus.webapp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

public class TestGenerateXml extends TestCase {
	public void testGenerateXmlSimple() throws Exception {
		long time = System.currentTimeMillis();
		
		FileInputStream fis = new FileInputStream("data/dogfood/data.1.nt");
		NxParser nxp = new NxParser(fis);
		
		FileOutputStream fos = new FileOutputStream("data/dogfood/data.1.basic.rdf");
		PrintWriter pw = new PrintWriter(fos);
		printRDFXML(nxp, pw);
		
		fis.close();
		pw.close();
		
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed " + (time1-time) + " ms");
	}
	
	public void testGenerateXmlStax() throws Exception {
		long time = System.currentTimeMillis();
		
		FileInputStream fis = new FileInputStream("data/dogfood/data.1.nt");
		NxParser nxp = new NxParser(fis);
		
		FileOutputStream fos = new FileOutputStream("data/dogfood/data.1.stax.rdf");
		PrintWriter pw = new PrintWriter(fos);
		printRdfXml(nxp, pw);
		
		fis.close();
		pw.close();
		
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed " + (time1-time) + " ms");
	}
	
	private static void printRDFXML(Iterator<Node[]> it, PrintWriter out) {
		out.println("<?xml version='1.0'?>");
		out.println("<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'>");

		Node oldsubj = null;
		List<Node[]> list = new ArrayList<Node[]>();
		while (it.hasNext()) {
			Node[] nx = it.next();
			Node subj = nx[0];

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
	}
	
	private static void printRDFXML(List<Node[]> list, PrintWriter out) {
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

	private static String escape(String s){
		String e;
		e = s.replaceAll("&", "&amp;");
		e = e.replaceAll("<", "&lt;");
		e = e.replaceAll(">", "&gt;");
		e = e.replaceAll("\"","&quot;");
		e = e.replaceAll("'","&apos;");
		return e;
	}
	
	private static void printRdfXml(Iterator<Node[]> it, PrintWriter out) throws XMLStreamException {		
		XMLOutputFactory factory = XMLOutputFactory.newInstance();
		factory.setProperty(factory.IS_REPAIRING_NAMESPACES, Boolean.TRUE);

		XMLStreamWriter ch;

		ch = factory.createXMLStreamWriter(out);

		ch.writeStartDocument("utf-8", "1.0");

		ch.writeStartElement("rdf:RDF");
		ch.writeNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

		Node oldsubj = null;
		List<Node[]> list = new ArrayList<Node[]>();
		while (it.hasNext()) {
			Node[] nx = it.next();
			Node subj = nx[0];

			// new subject encountered
			if (oldsubj != null && !subj.equals(oldsubj)) {
				printRdfXml(list, ch);
				list = new ArrayList<Node[]>();
			}

			list.add(nx);

			oldsubj = subj;

		}

		if (!list.isEmpty()) {
			printRDFXML(list, out);
		}

		out.println("</rdf:RDF>");
	}
	
	private static void printRdfXml(List<Node[]> list, XMLStreamWriter ch) throws XMLStreamException {
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
			}
			ch.writeEndElement();
		}
		
		ch.writeEndElement();
	}
}
