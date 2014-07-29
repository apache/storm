package backtype.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.ParseException;

import backtype.storm.utils.Utils;

/**
 * A utility to help run {@link Tool}s
 *
 * <p><code>ToolRunner</code> can be used to run classes extending the
 * <code>Tool</code> abstract class.  It works in conjunction with
 * {@link GenericOptionsParser} to parse the <a
 * href="{@docRoot}/backtype/storm/GenericOptionsParser.html#GenericOptions">
 * generic storm command line arguments</a> and modifies the
 * <code>Config</code> of the <code>Tool</code>.  The
 * application-specific options are passed along without being
 * modified.
 *
 * @see Tool
 * @see GenericOptionsParser
 */

public class ToolRunner {
    static final Logger LOG = LoggerFactory.getLogger(ToolRunner.class);

    public static void run(Tool tool, String[] args) {
        run(tool.getConf(), tool, args);
    }

    public static void run(Config conf, Tool tool, String[] args) {
        try {
            if (conf == null) {
                conf = new Config();
                conf.putAll(Utils.readStormConfig());
            }

            GenericOptionsParser parser = new GenericOptionsParser(conf, args);
            tool.setConf(conf);

            System.exit(tool.run(parser.getRemainingArgs()));
        } catch (ParseException e) {
            LOG.error("Error parsing generic options: {}", e.getMessage());
            GenericOptionsParser.printGenericCommandUsage(System.err);
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Error running tool", e);
            System.exit(1);
        }
    }
}
