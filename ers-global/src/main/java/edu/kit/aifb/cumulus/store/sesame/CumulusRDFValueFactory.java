package edu.kit.aifb.cumulus.store.sesame;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Quad;
import org.semanticweb.yars.nx.Triple;
import org.semanticweb.yars.nx.util.NxUtil;

public class CumulusRDFValueFactory extends ValueFactoryImpl {

	public BNode createBNode(org.semanticweb.yars.nx.BNode bn) {
		return createBNode(bn.toString());
	}
	
	public Literal createLiteral(org.semanticweb.yars.nx.Literal lit) {
		if (lit.getDatatype() != null && lit.getLanguageTag() != null) {
//			log.warn("unable to convert " + lit.toN3() + ", literal should not have language tag and datatype!");
			return null;
		}

		if (lit.getDatatype() != null)
			return createLiteral(lit.toString(), createURI(lit.getDatatype()));
		
		if (lit.getLanguageTag() != null)
			return createLiteral(lit.toString(), lit.getLanguageTag());
		
		Literal l = createLiteral(lit.toString());
		
		return l;
	}
	
	public URI createURI(org.semanticweb.yars.nx.Resource res) {
		return createURI(res.toString());
	}
	
	public Resource createResource(org.semanticweb.yars.nx.Node n) {
		if (n instanceof org.semanticweb.yars.nx.Resource)
			return createURI((org.semanticweb.yars.nx.Resource)n);
		if (n instanceof org.semanticweb.yars.nx.BNode)
			return createBNode((org.semanticweb.yars.nx.BNode)n);
//		log.warn("unknown resource type: " + n.toN3() + "(" + n.getClass() + ")");
		return null;
	}
	
	public Value createValue(Node n) {
		if (n instanceof org.semanticweb.yars.nx.Resource)
			return createURI((org.semanticweb.yars.nx.Resource)n);
		if (n instanceof org.semanticweb.yars.nx.BNode)
			return createBNode((org.semanticweb.yars.nx.BNode)n);
		if (n instanceof org.semanticweb.yars.nx.Literal)
			return createLiteral((org.semanticweb.yars.nx.Literal)n);
		
//		log.warn("unknown node type: " + n.toN3() + "(" + n.getClass() + ")");
		return null;
	}
	
	public Statement createStatement(Node[] nx) {
		if (nx.length == 3)
			return createStatement(Triple.fromArray(nx));
		if (nx.length == 4)
			return createStatement(Quad.fromArray(nx));
		
		throw new IllegalArgumentException("argument should have length 3 or 4");
	}
	
	public Statement createStatement(Triple t){
		return createStatement(createResource(t.getSubject()), createURI((org.semanticweb.yars.nx.Resource)t.getPredicate()), createValue(t.getObject()));
	}
	
	public Statement createStatement(Quad q) {
		return createStatement(createResource(q.getSubject()), createURI((org.semanticweb.yars.nx.Resource)q.getPredicate()), createValue(q.getObject()), createURI((org.semanticweb.yars.nx.Resource)q.getContext()));
	}
	
	public org.semanticweb.yars.nx.BNode createNxBNode(BNode bn) {
		return new org.semanticweb.yars.nx.BNode(bn.toString());
	}

	public org.semanticweb.yars.nx.Literal createNxLiteral(Literal lit) {
		if (lit.getDatatype() != null)
			return new org.semanticweb.yars.nx.Literal(NxUtil.escapeForNx(lit.stringValue()), createNxResource(lit.getDatatype()));
		if (lit.getLanguage() != null)
			return new org.semanticweb.yars.nx.Literal(NxUtil.escapeForNx(lit.stringValue()), lit.getLanguage());
		return new org.semanticweb.yars.nx.Literal(NxUtil.escapeForNx(lit.stringValue()));
	}

	public org.semanticweb.yars.nx.Resource createNxResource(URI u) {
		return new org.semanticweb.yars.nx.Resource(u.toString());
	}
	
	public Node createNode(Value v) {
		if (v == null)
			return null;

		if (v instanceof URI)
			return createNxResource((URI)v);
		else if (v instanceof BNode)
			return createNxBNode((BNode)v);
		else if (v instanceof Literal)
			return createNxLiteral((Literal)v);
		else
			throw new IllegalArgumentException("v should be of type URI, BNode or Literal");
	}

	public Node[] createNodes(Value... vs) {
		Node[] nx = new Node[vs.length];
		for (int i = 0; i < nx.length; i++)
			nx[i] = createNode(vs[i]);
		return nx;
	}

//	public Node[] createNodes(org.openrdf.model.Resource subj, URI pred, Value obj, org.openrdf.model.Resource... contexts) {
//		return createNodes(Util.concat(new Value[] { subj, pred, obj }, contexts));
//	}
}
