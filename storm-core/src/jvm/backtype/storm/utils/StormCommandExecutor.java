package backtype.storm.utils;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.lang.SystemUtils;

/**
 * Created by pshah on 7/17/15.
 */
public class StormCommandExecutor {

    //boolean isWindows;
    //String pathSeparator;
    private String stormHomeDirectory;
    private String userConfDirectory;
    private String stormConfDirectory;
    private String clusterConfDirectory;
    private String stormLibDirectory;
    private String stormBinDirectory;
    private String stormLog4jConfDirectory;
    private String configFile;
    private String javaCommand;
    private List<String> configOptions = new ArrayList<String>();
    private String stormExternalClasspath;
    private String stormExternalClasspathDaemon;
    private final String[] COMMANDS = {"jar", "kill", "shell", "nimbus", "ui",
            "logviewer",
            "drpc", "supervisor", "localconfvalue",
            "remoteconfvalue", "repl", "classpath",
            "activate", "deactivate", "rebalance", "help",
            "list", "dev-zookeeper", "version", "monitor",
            "upload-credentials", "get-errors" };

    public static void main (String[] args) {
        System.out.println("Executing StormCommandExecutor main method");
        StormCommandExecutor stormCommandExecutor = new StormCommandExecutor();
        if (args.length == 0) {
            System.exit(-1);
        }
        //stormCommandExecutor.initialize();
        for (String arg: args) {
            System.out.println("Argument ++ is " + arg);
        }
    }

    StormCommandExecutor() {

    }
    private void initialize() {
        //this.isWindows = System.getProperty("os.name").startsWith("Windows");
        //this.pathSeparator = System.getProperty("path.separator");
        String fileSeparator = System .getProperty("fileSeparator");
        this.stormHomeDirectory = System.getenv("STORM_BASE_DIR");
        this.userConfDirectory = System.getProperty("user.home") +
                fileSeparator + "" +
                ".storm";
        this.stormConfDirectory = System.getenv("STORM_CONF_DIR");
        this.clusterConfDirectory = this.stormConfDirectory == null ?  (this
                .stormHomeDirectory + fileSeparator + "conf") : this
                .stormConfDirectory;
        File f = new File(this.userConfDirectory + fileSeparator + "storm" +
                ".yaml");
        if (!f.isFile()) {
            this.userConfDirectory = this.clusterConfDirectory;
        }
        this.stormLibDirectory = this.stormHomeDirectory + fileSeparator +
                "lib";
        this.stormBinDirectory = this.stormHomeDirectory + fileSeparator +
                "bin";
        this.stormLog4jConfDirectory = this.stormHomeDirectory +
                fileSeparator + "log4j2";
        if (System.getenv("JAVA_HOME") != null) {
            this.javaCommand = System.getenv("JAVA_HOME") + fileSeparator +
                    "bin" + fileSeparator + "java";
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
}
