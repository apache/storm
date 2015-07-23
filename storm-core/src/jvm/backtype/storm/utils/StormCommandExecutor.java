package backtype.storm.utils;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
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
    final String[] COMMANDS = {"jar", "kill", "shell", "nimbus", "ui",
            "logviewer",
            "drpc", "supervisor", "localconfvalue",
            "remoteconfvalue", "repl", "classpath",
            "activate", "deactivate", "rebalance", "help",
            "list", "dev-zookeeper", "version", "monitor",
            "upload-credentials", "get-errors"};

    public static void main(String[] args) {
        System.out.println("Executing StormCommandExecutor main method");
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
        this.fileSeparator = System .getProperty ("fileSeparator");
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
        System.out.println("this.stormHomeDirectory is " + this
                .stormHomeDirectory);
        System.out.println("this.userConfDirectory is " + this
                .userConfDirectory);
        System.out.println("this.stormConfDirectory is " + this
                .stormConfDirectory);
        //System.getProperties().list(System.out);
    }

    void execute (String[] args) {
        if (args.length == 0) {
            this.executeHelpCommand();
            System.exit(-1);
        }
        return;
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
