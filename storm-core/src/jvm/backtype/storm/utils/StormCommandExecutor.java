package backtype.storm.utils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

/**
 * Created by pshah on 7/17/15.
 */
abstract class StormCommandExecutor {

    String stormHomeDirectory;
    String userConfDirectory;
    String stormConfDirectory;
    String clusterConfDirectory;
    String stormLibDirectory;
    String stormBinDirectory;
    String stormLog4jConfDirectory;
    String configFile;
    String javaCommand;
    List<String> configOptions = new ArrayList<String>();
    String stormExternalClasspath;
    String stormExternalClasspathDaemon;
    String fileSeparator;
    final List<String> COMMANDS = Arrays.asList("jar", "kill", "shell",
            "nimbus", "ui", "logviewer", "drpc", "supervisor",
            "localconfvalue",  "remoteconfvalue", "repl", "classpath",
            "activate", "deactivate", "rebalance", "help",  "list",
            "dev-zookeeper", "version", "monitor", "upload-credentials",
            "get-errors");

    public static void main(String[] args) {
        for (String arg : args) {
            System.out.println("Argument ++ is " + arg);
        }
        StormCommandExecutor stormCommandExecutor;
        if (System.getProperty("os.name").startsWith("Windows")) {
            stormCommandExecutor = new WindowsStormCommandExecutor();
        } else {
            stormCommandExecutor = new UnixStormCommandExecutor();
        }
        stormCommandExecutor.initialize();
        stormCommandExecutor.execute(args);
    }

    StormCommandExecutor() {

    }

    abstract void initialize();
    abstract void execute(String[] args);
}

class UnixStormCommandExecutor extends StormCommandExecutor {

    UnixStormCommandExecutor() {

    }

    void initialize() {
        Collections.sort(this.COMMANDS);
        this.fileSeparator = System .getProperty ("file.separator");
        this.stormHomeDirectory = System.getenv("STORM_BASE_DIR");
        this.userConfDirectory = System.getProperty("user.home") +
                this.fileSeparator + "" +
                ".storm";
        this.stormConfDirectory = System.getenv("STORM_CONF_DIR");
        this.clusterConfDirectory = this.stormConfDirectory == null ?  (this
                .stormHomeDirectory + this.fileSeparator + "conf") : this
                .stormConfDirectory;
        File f = new File(this.userConfDirectory + this.fileSeparator +
                "storm" +
                ".yaml");
        if (!f.isFile()) {
            this.userConfDirectory = this.clusterConfDirectory;
        }
        this.stormLibDirectory = this.stormHomeDirectory + this.fileSeparator +
                "lib";
        this.stormBinDirectory = this.stormHomeDirectory + this.fileSeparator +
                "bin";
        this.stormLog4jConfDirectory = this.stormHomeDirectory +
                this.fileSeparator + "log4j2";
        if (System.getenv("JAVA_HOME") != null) {
            this.javaCommand = System.getenv("JAVA_HOME") + this.fileSeparator +
                    "bin" + this.fileSeparator + "java";
            if (!(new File(this.javaCommand).exists())) {
                System.out.println("ERROR:  JAVA_HOME is invalid.  Could not " +
                        "find " + this.javaCommand);
                System.exit(1);
            }
        } else {
            this.javaCommand = "java";
        }
        this.stormExternalClasspath = System.getenv("STORM_EXT_CLASSPATH");
        this.stormExternalClasspathDaemon = System.getenv
                ("STORM_EXT_CLASSPATH_DAEMON");
        if (!(new File(this.stormLibDirectory).exists())) {
            System.out.println("******************************************");
            System.out.println("The storm client can only be run from within " +
                    "a release. " + "You appear to be trying to run the client" +
                    " from a checkout of Storm's source code.");
            System.out.println("You can download a Storm release at " +
                    "http://storm-project.net/downloads.html");
            System.out.println("******************************************");
            System.exit(1);
        }
        //System.getProperties().list(System.out);
    }

    void execute (String[] args) {
        if (args.length == 0) {
            this.printUsage();
            System.exit(-1);
        }
        List<String> commandArgs = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            if (args[i] == "-c") {
                this.configOptions.add(args[++i]);
            } else if (args[i] == "--config") {
                this.configFile = args[++i];
            } else {
                commandArgs.add(args[i]);
            }
        }
        if ((commandArgs.size() == 0)  || (!this.COMMANDS.contains
                (commandArgs.get(0)))) {
            System.out.println("Unknown command: [storm " + StringUtils.join
                    (args, " ") +  "]");
            this.printUsage();
            System.exit(254);

        }

    }

    private void printUsage() {
        String commands = StringUtils.join(this.COMMANDS, "\n\t");
        System.out.println("Commands:\n\t" + commands);
        System.out.println("\nHelp: \n\thelp \n\thelp <command>\n");
        System.out.println("Documentation for the storm client can be found" +
                " at "  +
                "http://storm.incubator.apache" +
                ".org/documentation/Command-line-client.html\n");
        System.out.println("Configs can be overridden using one or more -c " +
                "flags, e.g. " +
                "\"storm list -c nimbus.host=nimbus.mycompany.com\"\n");
    }

    private void executeHelpCommand() {
        System.out.println("Print storm help here");
    }

}

class WindowsStormCommandExecutor extends StormCommandExecutor {

    WindowsStormCommandExecutor() {

    }

    void initialize() {
        return;
    }

    void execute (String[] args) {
        return;
    }

}
