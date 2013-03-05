package edu.kit.aifb.cumulus.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

public class SamplePatterns {
	static String[] PARAMS = { "s", "p", "o" };

	private static Node urlDecode(Node n) {
		if (n instanceof Resource)
			try {
				return new Resource(URLDecoder.decode(n.toString(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				return n;
			}
		return n;
	}

	public static void main (String[] args) throws IOException, org.semanticweb.yars.nx.parser.ParseException, ClassNotFoundException{
		Option inputO = new Option("i", "name of file to read, - for stdin");
		inputO.setArgs(1);
		
		Option outputO = new Option("o", "name of file to write, - for stdout");
		outputO.setArgs(1);

		Option sampleO = new Option("s", "sample probability (0...1)");
		sampleO.setArgs(1);
		sampleO.setRequired(true);
		
		Option patternsO = new Option("p", "patterns to sample (s,p,o,sp,so,po,spo)");
		patternsO.setArgs(1);
		
		Option helpO = new Option("h", "print help");
		
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(outputO);
		options.addOption(sampleO);
		options.addOption(patternsO);
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		

		float sample = Float.parseFloat(cmd.getOptionValue("s"));

		InputStream in = System.in;
		PrintStream out = System.out;
		
		if (cmd.hasOption("i")) {
			if (cmd.getOptionValue("i").equals("-")) {
				in = System.in;
			} else {
				in = new FileInputStream(cmd.getOptionValue("i"));
			}
		}
		
		if (cmd.hasOption("o")) {
			if ("-".equals(cmd.getOptionValue("o"))) {
				out = System.out;
			} else {
				out = new PrintStream(new FileOutputStream(cmd.getOptionValue("o")));
			}
		}

		Set<String> patterns = new HashSet<String>();
		if (cmd.hasOption("p")) {
			String[] ps = cmd.getOptionValue("p").split(",");
			for (String s : ps)
				patterns.add(s.trim().toLowerCase());
		}
		else
			patterns.addAll(Arrays.asList("s", "o", "sp", "so", "po"));

		Random r = new Random(1);

		NxParser nxp = new NxParser(in);
		
		int count = 0;
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			nx[0] = urlDecode(nx[0]);
			nx[1] = urlDecode(nx[1]);
			nx[2] = urlDecode(nx[2]);

			if (r.nextFloat() < sample) {
				Node[] pattern = new Node[3];
				int j = 0;

				for (int i = 0; i < 3; i++) {
					if (r.nextBoolean() != true) {
						pattern[i] = null;
					} else {
						pattern[i] = nx[i];
						j++;
					}
				}

				StringBuilder patternId = new StringBuilder();
				if (pattern[0] != null)
					patternId.append("s");
				if (pattern[1] != null)
					patternId.append("p");
				if (pattern[2] != null)
					patternId.append("o");
				
				// disallow ?s ?p ?o
//				if (j > 0) { // && !(pattern[0] == null && pattern[1] != null && pattern[2] == null)) {
				if (patterns.contains(patternId.toString())) {
					StringBuffer sb = new StringBuffer();
					sb.append("/query?");
					for (int i = 0; i < 3; i++) {
						if (pattern[i] != null) {
							sb.append(PARAMS[i]);
							sb.append("=");
							sb.append(URLEncoder.encode(pattern[i].toN3(), "utf-8"));
							sb.append("&");
						}
					}

					String tp = sb.toString();
					if (tp.endsWith("&")) {
						tp = tp.substring(0, tp.length()-1);
					}

					count++;
					if (count % 50000 == 0)
						System.out.println(count);
					out.println(tp);
				}
			}
			
		}

		out.close();
	}
}
