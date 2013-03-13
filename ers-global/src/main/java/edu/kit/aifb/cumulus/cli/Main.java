package edu.kit.aifb.cumulus.cli;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Resource;

public class Main {
	private static final String USAGE = "USAGE: edu.kit.aifb.cumulus.cli.Main <utility> [options...]";
	private static final String PREFIX = "edu.kit.aifb.cumulus.cli.";

	public static void main(String[] args) throws UnsupportedEncodingException {
		try {
			if (args.length < 1) {
				StringBuffer sb = new StringBuffer();
				sb.append("where <utility> one of");
				sb.append("\n\tLoad            Load and index triples/quads");
				sb.append("\n\tLoadRedirects   Load redirects (for proxy mode)");
				usage(sb.toString());
			}
			
			Class cls = Class.forName(PREFIX + args[0]);
			Method mainMethod = cls.getMethod("main", new Class[] { String[].class });

			String[] mainArgs = new String[args.length - 1];
			System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);
			
			long time = System.currentTimeMillis();
			mainMethod.invoke(null, new Object[] { mainArgs });
			long time1 = System.currentTimeMillis();
			System.err.println("time elapsed " + (time1-time) + " ms");

		} catch (Throwable e) {
			e.printStackTrace();
			usage(e.toString());
		}
	}
	private static void usage(String msg) {
		System.err.println(USAGE);
		System.err.println(msg);
		System.exit(-1);
	}
}
