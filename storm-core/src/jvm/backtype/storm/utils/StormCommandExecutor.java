package backtype.storm.utils;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

    void callMethod(String command, List<String> args) {
        Class implementation = this.getClass();
        String methodName = command.replace("-", "") + "Command";
        try {
            Method method = implementation.getDeclaredMethod(methodName, List
                    .class);
            method.invoke(this, args);
        } catch (NoSuchMethodException ex) {
            System.out.println("No such method exception occured while trying" +
                    " to run storm method " + command);
        } catch (IllegalAccessException ex) {
            System.out.println("Illegal access exception occured while trying" +
                    " to run storm method " + command);
        } catch (IllegalArgumentException ex) {
            System.out.println("Illegal argument exception occured while " +
                    "trying" + " to run storm method " + command);
        } catch (InvocationTargetException ex) {
            System.out.println("Invocation target exception occured while " +
                    "trying" + " to run storm method " + command);
        }
    }
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
        this.callMethod(commandArgs.get(0), commandArgs.subList(1,
                commandArgs.size()));

    }

    void jarCommand(List<String> args) {
        System.out.println("Called jarCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void killCommand(List<String> args) {
        System.out.println("Called killCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void shellCommand(List<String> args) {
        System.out.println("Called shellCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void nimbusCommand(List<String> args) {
        System.out.println("Called nimbusCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void uiCommand(List<String> args) {
        System.out.println("Called uiCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void logviewerCommand(List<String> args) {
        System.out.println("Called logviewerCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void drpcCommand(List<String> args) {
        System.out.println("Called drpcCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void supervisorCommand(List<String> args) {
        System.out.println("Called supervisorCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void localconfvalueCommand(List<String> args) {
        System.out.println("Called localconfvalueCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void remoteconfvalueCommand(List<String> args) {
        System.out.println("Called remoteconfvalueCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void replCommand(List<String> args) {
        System.out.println("Called replCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void classpathCommand(List<String> args) {
        System.out.println("Called classpathCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void activateCommand(List<String> args) {
        System.out.println("Called activateCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void deactivateCommand(List<String> args) {
        System.out.println("Called deactivateCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void rebalanceCommand(List<String> args) {
        System.out.println("Called rebalanceCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void helpCommand(List<String> args) {
        System.out.println("Called helpCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void listCommand(List<String> args) {
        System.out.println("Called listCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void devzookeeperCommand(List<String> args) {
        System.out.println("Called devzookeeperCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void versionCommand(List<String> args) {
        System.out.println("Called versionCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void monitorCommand(List<String> args) {
        System.out.println("Called monitorCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void uploadcredentialsCommand(List<String> args) {
        System.out.println("Called uploadcredentialsCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void geterrorsCommand(List<String> args) {
        System.out.println("Called geterrorsCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
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
