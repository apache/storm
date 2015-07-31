package backtype.storm.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

import clojure.lang.IFn;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

/**
 * Created by pshah on 7/17/15.
 */
abstract class StormCommandExecutor {
    final String NIMBUS_CLASS = "backtype.storm.daemon.nimbus";
    String stormHomeDirectory;
    String userConfDirectory;
    String stormConfDirectory;
    String clusterConfDirectory;
    String stormLibDirectory;
    String stormBinDirectory;
    String stormLog4jConfDirectory;
    String configFile = "";
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

    public static void main (String[] args) {
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

    StormCommandExecutor () {

    }

    abstract void initialize ();

    abstract void execute (String[] args);

    void callMethod (String command, List<String> args) {
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

    UnixStormCommandExecutor () {

    }

    void initialize () {
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
                "storm.yaml");
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

    String getConfigOptions() {
        String configOptions = "-Dstorm.options=";
        //TODO  - do urlencode here. python does quote_plus to each configoption
        return configOptions + StringUtils.join(this.configOptions, ',');

    }

    List<String> getJarsFull (String directory) {
        List<String> fullJarFiles = new ArrayList<String>();
        File file = new File(directory);
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().endsWith(".jar")) {
                    fullJarFiles.add(f.getPath());
                }
            }
        }
        return fullJarFiles;
    }

    String getClassPath (List<String> extraJars, boolean daemon) {
        List<String> classPaths = this.getJarsFull(this.stormHomeDirectory);
        classPaths.addAll(this.getJarsFull(this.stormLibDirectory));
        classPaths.addAll(this.getJarsFull(this.stormHomeDirectory + this
                .fileSeparator + "extlib"));
        if (daemon == true) {
            classPaths.addAll(this.getJarsFull(this.stormHomeDirectory + this
                    .fileSeparator + "extlib-daemon"));
        }
        if (this.stormExternalClasspath != null) {
            classPaths.add(this.stormExternalClasspath);
        }
        if (this.stormExternalClasspathDaemon != null) {
            classPaths.add(this.stormExternalClasspathDaemon);
        }
        classPaths.addAll(extraJars);
        return StringUtils.join(classPaths, System.getProperty("path" +
                ".separator"));
    }

    String confValue (String name, List<String> extraPaths, boolean daemon) {
        // The original code from python started a process that started a jvm
        // with backtype.storm.command.config_value main method that would
        // read the conf value and print it out to an output stream. python
        // tapped on to the output stream of that subprocess and returned the
        // confvalue for the name. Because the pythong code has been shipped
        // to java now it should not spawn a new process which is a jvm since
        // we are already in jvm. Instead it should just be doing as the code
        // commeneted below.
        // However looking at the pythong code it was
        // starting a jvm with -cp argument that had classpaths which might
        // not be available to this java process. Hence there is a chance
        // that the below code might break existing scripts. As a result I
        // have decided to still spawn a new process from java just like
        // python with similar classpaths being constructed for the jvm
        // execution
        /*IFn fn = Utils.loadClojureFn("backtype.storm.config",
                "read-storm-config");
        Object o = fn.invoke();
        return ((Map) o).get(name).toString();*/
        String confValue = "";
        ProcessBuilder processBuilder = new ProcessBuilder(this.javaCommand,
                "-client", this.getConfigOptions(), "-Dstorm.conf.file=" +
                this.configFile, "-cp", this.getClassPath(extraPaths, daemon),
                "backtype.storm.command.config_value", name);
        BufferedReader br;
        try {
            Process process = processBuilder.start();
            br = new BufferedReader(new InputStreamReader(process
                    .getInputStream(), StandardCharsets.UTF_8));
            process.waitFor();
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(" ");
                if ("VALUE:".equals(tokens[0])) {
                    confValue = StringUtils.join(Arrays.copyOfRange(tokens, 1,
                            tokens.length), " ");
                    break;
                }
            }
            br.close();
        } catch (Exception ex) {
            System.out.println("Exception occured while starting process via " +
                    "processbuilder " + ex.getMessage());
        }
        return confValue;
    }

    void executeStormClass (String className, String jvmType, List<String>
            jvmOptions, List<String> extraJars, List<String> args, boolean
            fork, boolean daemon, String daemonName) {
        List<String> extraPaths = new ArrayList<>();
        extraPaths.add(this.clusterConfDirectory);
        String stormLogDirectory = this.confValue("storm.log.dir",
                extraPaths, daemon);
        if ((stormLogDirectory == null) || ("".equals(stormLogDirectory)) ||
                ("nil".equals(stormLogDirectory))) {
            stormLogDirectory = this.stormHomeDirectory + this.fileSeparator
                    + "logs";
        }
        List<String> commandList = new ArrayList<String>();
        commandList.add(this.javaCommand);
        commandList.add(jvmType);
        commandList.add("-Ddaemon.name=" + daemonName);
        commandList.add(this.getConfigOptions());
        commandList.add("-Dstorm.home=" + this.stormHomeDirectory);
        commandList.add("-Dstorm.log.dir=" + stormLogDirectory);
        commandList.add("-Djava.library.path=" + this
                .confValue("java.library.path", extraJars, daemon));
        commandList.add("-Dstorm.conf.file=" + this.configFile);
        commandList.add("-cp");
        commandList.add(this.getClassPath(extraJars, daemon));
        commandList.addAll(jvmOptions);
        commandList.add(className);
        commandList.addAll(args);
        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.inheritIO();
        try {
            Process process = processBuilder.start();
            System.out.println("Executing the command: ");
            String commandLine = StringUtils.join(commandList, " ");
            System.out.println(commandLine);
            if (daemon == true) {
                Runtime.getRuntime().addShutdownHook(new ShutdownHookThread
                        (process, commandLine));
            }
            System.out.println("Waiting for subprocess to finish");
            process.waitFor();
            System.out.println("subprocess finished");
            System.out.println("Exit value from subprocess is :" + process
                    .exitValue());
        } catch (Exception ex) {
            System.out.println("Exception occured while starting process via " +
                    "processbuilder " + ex.getMessage());
        }
    }

    void jarCommand (List<String> args) {
        System.out.println("Called jarCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void killCommand (List<String> args) {
        System.out.println("Called killCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void shellCommand (List<String> args) {
        System.out.println("Called shellCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void nimbusCommand (List<String> args) {
        System.out.println("Called nimbusCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        List<String> jvmOptions = new ArrayList<String>();
        List<String> extraPaths = new ArrayList<String>();
        extraPaths.add(this.clusterConfDirectory);
        String nimbusOptions = this.confValue("nimbus.childopts", extraPaths,
                true);
        // below line is different from original python script storm.py where
        // it called parse_args method on nimbusOptions. Now we just call a
        // split with a space.  Hence this will have different behavior and
        // a buggy one if the nimbusOptions string in the config file has a
        // space. TODO need to fix this
        jvmOptions.addAll(Arrays.asList(nimbusOptions.split(" ")));
        jvmOptions.add("-Dlogfile.name=nimbus.log");
        jvmOptions.add("-Dlog4j.configurationFile=" + this
                .getLog4jConfigDirectory() + this.fileSeparator + "cluster" +
                ".xml");
        this.executeStormClass(this.NIMBUS_CLASS, "-server", jvmOptions,
                extraPaths, new ArrayList<String>(), false, true, "nimbus");
        return;
    }

    void uiCommand (List<String> args) {
        System.out.println("Called uiCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void logviewerCommand (List<String> args) {
        System.out.println("Called logviewerCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void drpcCommand (List<String> args) {
        System.out.println("Called drpcCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void supervisorCommand (List<String> args) {
        System.out.println("Called supervisorCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void localconfvalueCommand (List<String> args) {
        System.out.println("Called localconfvalueCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void remoteconfvalueCommand (List<String> args) {
        System.out.println("Called remoteconfvalueCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void replCommand (List<String> args) {
        System.out.println("Called replCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void classpathCommand (List<String> args) {
        System.out.println("Called classpathCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void activateCommand (List<String> args) {
        System.out.println("Called activateCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void deactivateCommand (List<String> args) {
        System.out.println("Called deactivateCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void rebalanceCommand (List<String> args) {
        System.out.println("Called rebalanceCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void helpCommand (List<String> args) {
        System.out.println("Called helpCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void listCommand (List<String> args) {
        System.out.println("Called listCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void devzookeeperCommand (List<String> args) {
        System.out.println("Called devzookeeperCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void versionCommand (List<String> args) {
        System.out.println("Called versionCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void monitorCommand (List<String> args) {
        System.out.println("Called monitorCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void uploadcredentialsCommand (List<String> args) {
        System.out.println("Called uploadcredentialsCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    void geterrorsCommand (List<String> args) {
        System.out.println("Called geterrorsCommand using reflection");
        System.out.println("Arguments are : ");
        for (String s: args) {
            System.out.println(s);
        }
        return;
    }

    String getLog4jConfigDirectory () {
        List<String> extraPaths = new ArrayList<String>();
        extraPaths.add(this.clusterConfDirectory);
        String log4jDirectory = this.confValue("storm.logback.conf.dir",
                extraPaths, true);
        if ((log4jDirectory == null) || ("".equals(log4jDirectory)) ||
                ("nil".equals(log4jDirectory))) {
            log4jDirectory = this.stormLog4jConfDirectory;
        }
        return log4jDirectory;
    }

    private void printUsage () {
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

    private void executeHelpCommand () {
        System.out.println("Print storm help here");
    }

}

class WindowsStormCommandExecutor extends StormCommandExecutor {

    WindowsStormCommandExecutor () {

    }

    void initialize () {
        return;
    }

    void execute (String[] args) {
        return;
    }

}

class ShutdownHookThread extends Thread {
    private Process process;
    String commandLine;
    ShutdownHookThread (Process process, String commandLine) {
        this.process = process;
        this.commandLine = commandLine;
    }

    public void run () {
        System.out.println("Executing the shutdown hook for " +
                "StormCommandExecutor subprocess");
        if (this.process != null) {
            System.out.println("Killing the sub-process for command: " +
                    (this.commandLine != null ? this.commandLine : "Empty " +
                    "commandLine found"));
            this.process.destroy();
        } else{
            System.out.println("Null process object found. No process to kill");
        }
    }
}
