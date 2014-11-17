package backtype.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.yaml.snakeyaml.Yaml;

/**
 * <code>GenericOptionsParser</code> is a utility to parse command line
 * arguments generic to Storm.
 *
 * <code>GenericOptionsParser</code> recognizes several standard command line
 * arguments, enabling applications to easily specify additional jar files,
 * configuration resources, data files etc.
 *
 * <h4 id="GenericOptions">Generic Options</h4>
 *
 * <p>
 * The supported generic options are:
 * </p>
 * <p>
 * <blockquote>
 * 
 * <pre>
 * -conf &lt;conf.xml&gt;                           load configurations from
 *                                            &lt;conf.xml&gt;
 * -conf &lt;conf.yaml&gt;                          load configurations from
 *                                            &lt;conf.yaml&gt;
 * -D &lt;key=value&gt;                             set &lt;key&gt; in configuration to
 *                                            &lt;value&gt; (preserve value's type)
 * -libjars &lt;comma separated list of jars&gt;    specify comma separated jars to be
 *                                            used by the submitted topology
 * </pre>
 * 
 * </blockquote>
 * </p>
 *
 * <b>Note:</b> The XML configuration file specified by <code>-conf</code> shall
 * be readable by Hadoop's <a href=
 * "http://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html"
 * ><code>Configuration</code></a> class. Also note that all configuration
 * values of an XML file will be treated as strings, and <b>not as specific
 * types</b>.
 *
 * <p>
 * The general command line syntax is:
 * </p>
 * <p>
 * <tt><pre>
 *     storm jar app.jar [genericOptions] [commandOptions]
 * </pre></tt>
 * </p>
 *
 * <p>
 * Generic command line arguments <strong>might</strong> modify
 * <code>Config</code> objects, given to constructors.
 * </p>
 *
 * <h4>Configuration priority</h4>
 *
 * The following list defines the priorities of different configuration sources,
 * in ascending order. Thus, if a configuration appears in more than one of
 * them, only the last one will take effect.
 *
 * <ul>
 * <li> <code>defaults.yaml</code> in classpath.
 * <li> <code>storm.yaml</code> in classpath.
 * <li>Configurations from files specified with the <code>-conf</code> option,
 * in the order of appearance.
 * <li>Configurations defined with the <code>-D</code> option, in order of
 * appearance.
 * </ul>
 *
 * <p>
 * The functionality is implemented using Commons CLI.
 * </p>
 *
 * @see Tool
 * @see ToolRunner
 */

public class GenericOptionsParser {
	static final Logger LOG = LoggerFactory
			.getLogger(GenericOptionsParser.class);

	static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String TOPOLOGY_LIB_PATH = "topology.lib.path";

	public static final String TOPOLOGY_LIB_NAME = "topology.lib.name";

	Config conf;

	CommandLine commandLine;

	// Order in this map is important for these purposes:
	// - configuration priority
	static final LinkedHashMap<String, OptionProcessor> optionProcessors = new LinkedHashMap<String, OptionProcessor>();

	public GenericOptionsParser(Config conf, String[] args)
			throws ParseException {
		this(conf, new Options(), args);
	}

	public GenericOptionsParser(Config conf, Options options, String[] args)
			throws ParseException {
		this.conf = conf;
		parseGeneralOptions(options, conf, args);
	}

	public String[] getRemainingArgs() {
		return commandLine.getArgs();
	}

	public Config getConfiguration() {
		return conf;
	}

	static Options buildGeneralOptions(Options opts) {
		Options r = new Options();

		for (Object o : opts.getOptions())
			r.addOption((Option) o);

		Option libjars = OptionBuilder
				.withArgName("paths")
				.hasArg()
				.withDescription(
						"comma separated jars to be used by the submitted topology")
				.create("libjars");
		r.addOption(libjars);
		optionProcessors.put("libjars", new LibjarsProcessor());

		Option conf = OptionBuilder.withArgName("configuration file").hasArg()
				.withDescription("an application configuration file")
				.create("conf");
		r.addOption(conf);
		optionProcessors.put("conf", new ConfFileProcessor());

		// Must come after `conf': this option is of higher priority
		Option extraConfig = OptionBuilder.withArgName("D").hasArg()
				.withDescription("extra configurations (preserving types)")
				.create("D");
		r.addOption(extraConfig);
		optionProcessors.put("D", new ExtraConfigProcessor());

		return r;
	}

	void parseGeneralOptions(Options opts, Config conf, String[] args)
			throws ParseException {
		opts = buildGeneralOptions(opts);
		CommandLineParser parser = new GnuParser();
		commandLine = parser.parse(opts, args, true);
		processGeneralOptions(conf, commandLine);
	}

	void processGeneralOptions(Config conf, CommandLine commandLine)
			throws ParseException {
		for (Map.Entry<String, OptionProcessor> e : optionProcessors.entrySet())
			if (commandLine.hasOption(e.getKey()))
				e.getValue().process(conf, commandLine);
	}

	static List<File> validateFiles(String pathList) throws IOException {
		List<File> l = new ArrayList<File>();

		for (String s : pathList.split(",")) {
			File file = new File(s);
			if (!file.exists())
				throw new FileNotFoundException("File `"
						+ file.getAbsolutePath() + "' does not exist");

			l.add(file);
		}

		return l;
	}

	public static void printGenericCommandUsage(PrintStream out) {
		String[] strs = new String[] {
				"Generic options supported are",
				"  -conf <conf.xml>                            load configurations from",
				"                                              <conf.xml>",
				"  -conf <conf.yaml>                           load configurations from",
				"                                              <conf.yaml>",
				"  -D <key>=<value>                            set <key> in configuration",
				"                                              to <value> (preserve value's type)",
				"  -libjars <comma separated list of jars>     specify comma separated",
				"                                              jars to be used by",
				"                                              the submitted topology", };
		for (String s : strs)
			out.println(s);
	}

	static interface OptionProcessor {
		public void process(Config conf, CommandLine commandLine)
				throws ParseException;
	}

	static class LibjarsProcessor implements OptionProcessor {
		@Override
		public void process(Config conf, CommandLine commandLine)
				throws ParseException {
			try {
				List<File> jarFiles = validateFiles(commandLine
						.getOptionValue("libjars"));
				Map<String, String> jars = new HashMap<String, String>(jarFiles.size());
				List<String> names = new ArrayList<String>(jarFiles.size());
				for (File f : jarFiles) {
					jars.put(f.getName(), f.getAbsolutePath());
					names.add(f.getName());
				}
				conf.put(TOPOLOGY_LIB_PATH, jars);
				conf.put(TOPOLOGY_LIB_NAME, names);

			} catch (IOException e) {
				throw new ParseException(e.getMessage());
			}
		}
	}

	static class ExtraConfigProcessor implements OptionProcessor {
		static final Yaml yaml = new Yaml();

		@Override
		public void process(Config conf, CommandLine commandLine)
				throws ParseException {
			for (String s : commandLine.getOptionValues("D")) {
				String[] keyval = s.split("=", 2);
				if (keyval.length != 2)
					throw new ParseException("Invalid option value `" + s + "'");

				conf.putAll((Map) yaml.load(keyval[0] + ": " + keyval[1]));
			}
		}
	}

	static class ConfFileProcessor implements OptionProcessor {
		static final Yaml yaml = new Yaml();

		static Map loadYamlConf(String f) throws IOException {
			InputStreamReader reader = null;
			try {
				FileInputStream fis = new FileInputStream(f);
				reader = new InputStreamReader(fis, UTF8);
				return (Map) yaml.load(reader);
			} finally {
				if (reader != null)
					reader.close();
			}
		}

		static Map loadConf(String f) throws IOException {
			if (f.endsWith(".yaml"))
				return loadYamlConf(f);
			throw new IOException("Unknown configuration file type: " + f
					+ " does not end with either .yaml");
		}

		@Override
		public void process(Config conf, CommandLine commandLine)
				throws ParseException {
			try {
				for (String f : commandLine.getOptionValues("conf")) {
					Map m = loadConf(f);
					if (m == null)
						throw new ParseException("Empty configuration file "
								+ f);
					conf.putAll(m);
				}
			} catch (IOException e) {
				throw new ParseException(e.getMessage());
			}
		}
	}
}
