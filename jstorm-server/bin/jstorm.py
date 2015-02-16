#!/usr/bin/python

import os
import sys
import random
import subprocess as sub
import getopt

def identity(x):
    return x

def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = sub.Popen(command,stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    return lines[0]

if sys.platform == "cygwin":
    normclasspath = cygpath
else:
    normclasspath = identity

CLIENT_CONF_FILE = ""
JSTORM_DIR = "/".join(os.path.realpath( __file__ ).split("/")[:-2])
JSTORM_CONF_DIR = os.getenv("JSTORM_CONF_DIR", JSTORM_DIR + "/conf" )
LOG4J_CONF = JSTORM_CONF_DIR + "/jstorm.log4j.properties"
CONFIG_OPTS = []
STATUS = 0


def check_java():
    check_java_cmd = 'which java'
    ret = os.system(check_java_cmd)
    if ret != 0:
        print("Failed to find java, please add java to PATH")
        sys.exit(-1)

def get_config_opts():
    global CONFIG_OPTS
    return "-Dstorm.options=" + (','.join(CONFIG_OPTS)).replace(' ', "%%%%")

def get_client_childopts():
    ret = (" -Dstorm.root.logger=INFO,stdout -Dlog4j.configuration=File:%s/conf/aloha_log4j.properties "  %JSTORM_DIR)
    if CLIENT_CONF_FILE != "":
        ret += (" -Dstorm.conf.file=" + CLIENT_CONF_FILE)
    return ret

def get_server_childopts(log_name):
    ret = (" -Dlogfile.name=%s -Dlog4j.configuration=File:%s"  %(log_name, LOG4J_CONF))
    return ret

if not os.path.exists(JSTORM_DIR + "/RELEASE"):
    print "******************************************"
    print "The jstorm client can only be run from within a release. You appear to be trying to run the client from a checkout of JStorm's source code."
    print "\nYou can download a JStorm release "
    print "******************************************"
    sys.exit(1)  

def get_jars_full(adir):
    files = os.listdir(adir)
    ret = []
    for f in files:
        if f.endswith(".jar"):
            ret.append(adir + "/" + f)
    return ret

def get_classpath(extrajars):
    ret = []
    ret.extend(get_jars_full(JSTORM_DIR))
    ret.extend(get_jars_full(JSTORM_DIR + "/lib"))
    ret.extend(extrajars)
    return normclasspath(":".join(ret))

def confvalue(name, extrapaths):
    command = [
        "java", "-client", "-Xms256m", "-Xmx256m", get_config_opts(), "-cp", get_classpath(extrapaths), "backtype.storm.command.config_value", name
    ]
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    print "Failed to get config " + name
    print errors
    print output

def print_localconfvalue(name):
    """Syntax: [jstorm localconfvalue conf-name]

    Prints out the value for conf-name in the local JStorm configs. 
    The local JStorm configs are the ones in ~/.jstorm/storm.yaml merged 
    in with the configs in defaults.yaml.
    """
    print name + ": " + confvalue(name, [JSTORM_CONF_DIR])

def print_remoteconfvalue(name):
    """Syntax: [jstorm remoteconfvalue conf-name]

    Prints out the value for conf-name in the cluster's JStorm configs. 
    The cluster's JStorm configs are the ones in $STORM-PATH/conf/storm.yaml 
    merged in with the configs in defaults.yaml. 

    This command must be run on a cluster machine.
    """
    print name + ": " + confvalue(name, [JSTORM_CONF_DIR])

def exec_storm_class(klass, jvmtype="-server", childopts="", extrajars=[], args=[]):
    nativepath = confvalue("java.library.path", extrajars)
    args_str = " ".join(map(lambda s: "\"" + s + "\"", args))
    command = "java " + jvmtype + " -Djstorm.home=" + JSTORM_DIR + " " + get_config_opts() + " -Djava.library.path=" + nativepath + " " + childopts + " -cp " + get_classpath(extrajars) + " " + klass + " " + args_str
    print "Running: " + command    
    global STATUS
    STATUS = os.system(command)

def jar(jarfile, klass, *args):
    """Syntax: [jstorm jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments. 
    The jstorm jars and configs in ~/.jstorm are put on the classpath. 
    The process is configured so that StormSubmitter 
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    childopts = "-Dstorm.jar=" + jarfile + get_client_childopts()
    exec_storm_class(
        klass,
        jvmtype="-client -Xms256m -Xmx256m",
        extrajars=[jarfile, JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        args=args,
        childopts=childopts)

def zktool(*args):
    """Syntax: [jstorm jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments. 
    The jstorm jars and configs in ~/.jstorm are put on the classpath. 
    The process is configured so that StormSubmitter 
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "com.alibaba.jstorm.zk.ZkTool",
        jvmtype="-client -Xms256m -Xmx256m",
        extrajars=[ JSTORM_CONF_DIR, CLIENT_CONF_FILE],
        args=args,
        childopts=childopts)

def kill(*args):
    """Syntax: [jstorm kill topology-name [wait-time-secs]]

    Kills the topology with the name topology-name. JStorm will 
    first deactivate the topology's spouts for the duration of 
    the topology's message timeout to allow all messages currently 
    being processed to finish processing. JStorm will then shutdown 
    the workers and clean up their state. You can override the length 
    of time JStorm waits between deactivation and shutdown.
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.kill_topology", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def activate(*args):
    """Syntax: [jstorm activate topology-name]

    Activates the specified topology's spouts.
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.activate", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def deactivate(*args):
    """Syntax: [jstorm deactivate topology-name]

    Deactivates the specified topology's spouts.
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.deactivate", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def rebalance(*args):
    """Syntax: [jstorm rebalance topology-name [-w wait-time-secs]]

    Sometimes you may wish to spread out where the workers for a topology 
    are running. For example, let's say you have a 10 node cluster running 
    4 workers per node, and then let's say you add another 10 nodes to 
    the cluster. You may wish to have JStorm spread out the workers for the 
    running topology so that each node runs 2 workers. One way to do this 
    is to kill the topology and resubmit it, but JStorm provides a "rebalance" 
    command that provides an easier way to do this.

    Rebalance will first deactivate the topology for the duration of the 
    message timeout  and then redistribute 
    the workers evenly around the cluster. The topology will then return to 
    its previous state of activation (so a deactivated topology will still 
    be deactivated and an activated topology will go back to being activated).
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.rebalance", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def restart(*args):
    """Syntax: [jstorm restart topology-name [conf]]
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.restart", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def nimbus():
    """Syntax: [jstorm nimbus]

    Launches the nimbus daemon. This command should be run under 
    supervision with a tool like daemontools or monit. 

    See Setting up a JStorm cluster for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    nimbus_classpath = confvalue("nimbus.classpath", cppaths)
    childopts = confvalue("nimbus.childopts", cppaths) + get_server_childopts("nimbus.log")
    exec_storm_class(
        "com.alibaba.jstorm.daemon.nimbus.NimbusServer", 
        jvmtype="-server", 
        extrajars=(cppaths+[nimbus_classpath]), 
        childopts=childopts)

def supervisor():
    """Syntax: [jstorm supervisor]

    Launches the supervisor daemon. This command should be run 
    under supervision with a tool like daemontools or monit. 

    See Setting up a JStorm cluster for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    childopts = confvalue("supervisor.childopts", cppaths) + get_server_childopts("supervisor.log")
    exec_storm_class(
        "com.alibaba.jstorm.daemon.supervisor.Supervisor", 
        jvmtype="-server", 
        extrajars=cppaths, 
        childopts=childopts)


def drpc():
    """Syntax: [jstorm drpc]

    Launches a DRPC daemon. This command should be run under supervision 
    with a tool like daemontools or monit. 

    See Distributed RPC for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    childopts = confvalue("drpc.childopts", cppaths) + get_server_childopts("drpc.log")
    exec_storm_class(
        "com.alibaba.jstorm.drpc.Drpc", 
        jvmtype="-server", 
        extrajars=cppaths, 
        childopts=childopts)

def print_classpath():
    """Syntax: [jstorm classpath]

    Prints the classpath used by the jstorm client when running commands.
    """
    print get_classpath([])

def print_commands():
    """Print all client commands and link to documentation"""
    print "jstorm command [--config client_storm.yaml] [command parameter]"
    print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
    print "\n\t[--config client_storm.yaml]\t\t optional, setting client's storm.yaml"
    print "\nHelp:", "\n\thelp", "\n\thelp <command>"
    print "\nDocumentation for the jstorm client can be found at https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation\n"

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if COMMANDS.has_key(command):
            print (COMMANDS[command].__doc__ or 
                  "No documentation provided for <%s>" % command)
        else:
           print "<%s> is not a valid command" % command
    else:
        print_commands()

def unknown_command(*args):
    print "Unknown command: [jstorm %s]" % ' '.join(sys.argv[1:])
    print_usage()

def metrics_Monitor(*args):
    """Syntax: [jstorm metricsMonitor topologyname bool]
    Enable or disable the metrics monitor of one topology.
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.metrics_monitor", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def list(*args):
    """Syntax: [jstorm list]

    List cluster information
    """
    childopts = get_client_childopts()
    exec_storm_class(
        "backtype.storm.command.list", 
        args=args, 
        jvmtype="-client -Xms256m -Xmx256m", 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

COMMANDS = {"jar": jar, "kill": kill, "nimbus": nimbus, "zktool": zktool,
            "drpc": drpc, "supervisor": supervisor, "localconfvalue": print_localconfvalue,
            "remoteconfvalue": print_remoteconfvalue, "classpath": print_classpath,
            "activate": activate, "deactivate": deactivate, "rebalance": rebalance, "help": print_usage,
	    "metricsMonitor": metrics_Monitor, "list": list, "restart": restart}

def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)

def parse_config_opts(args):
  curr = args[:]
  curr.reverse()
  config_list = []
  args_list = []
  
  while len(curr) > 0:
    token = curr.pop()
    if token == "-c":
      config_list.append(curr.pop())
    elif token == "--config":
      global CLIENT_CONF_FILE
      CLIENT_CONF_FILE = curr.pop()
    else:
      args_list.append(token)
  
  return config_list, args_list
    
def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    global CONFIG_OPTS
    config_list, args = parse_config_opts(sys.argv[1:])
    parse_config(config_list)
    COMMAND = args[0]
    ARGS = args[1:]
    if COMMANDS.get(COMMAND) == None:
        unknown_command(COMMAND)
        sys.exit(-1)
    if len(ARGS) != 0 and ARGS[0] == "help":
        print_usage(COMMAND)
        sys.exit(0)
    try:
        (COMMANDS.get(COMMAND, "help"))(*ARGS)
    except Exception, msg:
        print(msg)
        print_usage(COMMAND)
        sys.exit(-1)
    sys.exit(STATUS)

if __name__ == "__main__":
    check_java()
    main()

