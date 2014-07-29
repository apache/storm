package backtype.storm;

/**
 * A tool abstract class that supports handling of generic
 * command-line options.
 *
 * <p>Here is how a typical <code>Tool</code> is implemented:</p>
 * <p><blockquote><pre>
 *     public class TopologyApp extends Tool {
 *         {@literal @}Override
 *         public int run(String[] args) throws Exception {
 *             // Config processed by ToolRunner
 *             Config conf = getConf();
 *
 *             // Other setups go here
 *             String name = "topology";
 *             StormTopology topology = buildTopology(args);
 *             StormSubmitter.submitTopology(name, conf, topology);
 *             return 0;
 *         }
 *
 *         StormTopology buildTopology(String[] args) { ... }
 *
 *         public static void main(String[] args) throws Exception {
 *             // Use ToolRunner to handle generic command-line options
 *             ToolRunner.run(new TopologyApp(), args);
 *         }
 *     }
 * </pre></blockquote></p>
 *
 * @see GenericOptionsParser
 * @see ToolRunner
 */

public abstract class Tool {
    Config config;

    public abstract int run(String[] args) throws Exception;

    public Config getConf() {
        return config;
    }

    public void setConf(Config config) {
        this.config = config;
    }
}
