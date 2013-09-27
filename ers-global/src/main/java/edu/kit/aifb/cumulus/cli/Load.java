package edu.kit.aifb.cumulus.cli;

import java.io.File;
import java.io.IOException;

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
import edu.kit.aifb.cumulus.store.StoreException;

public class Load {

	public static void main(String[] args) throws StoreException, IOException {
		Option inputO = new Option("i", "name of file to read, - for stdin (but then need to specify -x option)");
		inputO.setArgs(1);

		Option hostsO = new Option("n", "Cassandra hosts as comma-separated list ('host1:port1,host2:port2,...') (default localhost:9160)");
		hostsO.setArgs(1);

		Option keyspaceO = new Option("g", "Cassandra keyspace / graph name (mandatory)");
		keyspaceO.setArgs(1);

		Option indexO = new Option("x", "index to create");
		indexO.setArgs(1);
		
		Option threadsO = new Option("t", "number of loading threads (defaults to min(1,|hosts|/1.5))");
		threadsO.setArgs(1);
		
		Option storageO = new Option("s", "storage layout to use (flat|super) (needs to match webapp configuration)");
		storageO.setArgs(1);
		
		Option batchO = new Option("b", "batch size [MB]");
		batchO.setArgs(1);
		
		Option batchF = new Option("f", "format ('nt', 'nq' or 'xml') (default: 'nt')");
		batchF.setArgs(1);

		Option helpO = new Option("h", "print help");
		
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(indexO);
		options.addOption(hostsO);
		options.addOption(keyspaceO);
		options.addOption(threadsO);
		options.addOption(storageO);
		options.addOption(batchO);
		options.addOption(batchF);
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

		String in = "stdin";
		
		if (cmd.hasOption("i")) {
			if ("-".equals(cmd.getOptionValue("i"))) {
				if (!cmd.hasOption("x")) {
					System.err.println("***ERROR: when reading from standard input, specify index with -x");
					return;
				}
			} else {
				in = cmd.getOptionValue("i");
			}
		}
		
		String hosts = "localhost:9160";
		if (cmd.hasOption("n")) {
			hosts = cmd.getOptionValue("n");
		}
		
		int threads = -1;
		if (cmd.hasOption("t")) {
			threads = Integer.parseInt(cmd.getOptionValue("t"));
		}
	
		String keyspace = "";
		if (cmd.hasOption("g")) { 
			keyspace = cmd.getOptionValue("g"); 
		}
		if( keyspace.isEmpty() ) { 
			System.err.println("Please pass the graph name as -g parameter"); 
			System.exit(-1);	
		}
	
		String format = "nt";
		if (cmd.hasOption("f")) {
			format = cmd.getOptionValue("f");
			if (!format.equals("nt") && !format.equals("nq") && !format.equals("xml")) {
				System.err.println("unknown input format!");
				return;
			}
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
		else {
			System.out.println("Default storage layout: FLAT");
			crdf = new CassandraRdfHectorFlatHash(hosts);
		}
		crdf.createKeyspace(keyspace, false);

	
		int batchSizeMB = 1;
		if (cmd.hasOption("b")) {
			batchSizeMB = Integer.parseInt(cmd.getOptionValue("b"));
		}
		crdf.setBatchSize(batchSizeMB);
		crdf.open();

		if (!cmd.hasOption("x")) {
			crdf.bulkLoad(new File(in), format, threads, keyspace);
		} else {
			if (in.equals("stdin")) {
				crdf.bulkLoad(System.in, format, cmd.getOptionValue("x"), threads, keyspace);
			} else {
				crdf.bulkLoad(new File(in), format, cmd.getOptionValue("x"), threads, keyspace);
			}
		}
		System.out.println("closing...");
		crdf.close();
		System.out.println("closed");
		System.exit(0); 
	}
}
