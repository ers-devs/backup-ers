package edu.kit.aifb.cumulus.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorHierHash;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorQuads;
import edu.kit.aifb.cumulus.store.StoreException;

public class LoadRedirects {

	public static void main(String[] args) throws StoreException, IOException {
		Option inputO = new Option("i", "name of redirects file to read, - for stdin");
		inputO.setArgs(1);

		Option hostsO = new Option("n", "Cassandra hosts as comma-separated list ('host1:port1,host2:port2,...') (default localhost:9160)");
		hostsO.setArgs(1);
		
		Option storageO = new Option("s", "storage layout to use (flat|super) (needs to match webapp configuration)");
		storageO.setArgs(1);

		Option keyspaceO = new Option("g", "Cassandra keyspace / graph name (mandatory)");
		keyspaceO.setArgs(1);

		Option helpO = new Option("h", "print help");
		
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(hostsO);
		options.addOption(storageO);
		options.addOption(keyspaceO);
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
		
		if (!cmd.hasOption("i")) {
			System.err.println("***ERROR: use -i to specify input file (or stdin)");
			return;
		}

		InputStream in = System.in;
		
		if (cmd.hasOption("i")) {
			in= new FileInputStream(cmd.getOptionValue("i"));
		}

		String hosts = "localhost:9160";
		if (cmd.hasOption("n")) {
			hosts = cmd.getOptionValue("n");
		}
		
		String keyspace = "";
		if (cmd.hasOption("g"))
			keyspace = cmd.getOptionValue("g");
		if ( keyspace.isEmpty() ) { 
			System.err.println("Please give graph name as -g paramater"); 
			System.exit(-1);
		}
		AbstractCassandraRdfHector crdf = null;
		
		if (cmd.hasOption("s")) {
			String sl = cmd.getOptionValue("s");
			System.out.println("storage layout: " + sl);
			if ("super".equals(sl))
				crdf = new CassandraRdfHectorHierHash(hosts);
			else if ("flat".equals(sl))
				crdf = new CassandraRdfHectorFlatHash(hosts);
			else {
				System.err.println("unknown storage layout");
				return;
			}
		}
		else
			crdf = new CassandraRdfHectorFlatHash(hosts);

		crdf.open();
		crdf.createKeyspace(keyspace, false);
		
		if (in != null) {
			try {
				((CassandraRdfHectorQuads)crdf).loadRedirects(in, keyspace);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("closing...");
		crdf.close();
		System.out.println("closed");
		System.exit(0); 
	}
}
