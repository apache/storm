#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from os.path import expanduser
import sys, tempfile
import json
import subprocess as sub
import re
import shlex

def is_windows():
    return sys.platform.startswith('win')

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

SLIDER_DIR = os.getenv('SLIDER_HOME', None)

if  SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
    print "Unable to find SLIDER_HOME. Please configure SLIDER_HOME before running storm-slider"
    sys.exit(1)

USER_CONF_DIR = os.path.expanduser("~/.storm")

if os.getenv('STORM_BASE_DIR', None) != None:
    STORM_DIR = os.getenv('STORM_BASE_DIR', None)
elif os.getenv('STORM_HOME', None) != None:
    STORM_DIR = os.getenv('STORM_HOME', None)
else:
    print "Either STORM_BASE_DIR or STORM_HOME must be set."
    sys.exit(1)

CMD_OPTS = {}
CONFIG_OPTS = []
JAR_JVM_OPTS = shlex.split(os.getenv('STORM_JAR_JVM_OPTS', ''))
pid = os.getpid()
CONFFILE = os.path.join(tempfile.gettempdir(),"storm."+str(pid)+".json")
SLIDER_CLIENT_CONF = os.path.join(SLIDER_DIR,'conf','slider-client.xml')
SLIDER_CMD = os.path.join(SLIDER_DIR,'bin','slider.py')
JAVA_HOME = os.getenv('JAVA_HOME', None)
JAVA_CMD= 'java' if not JAVA_HOME else os.path.join(JAVA_HOME, 'bin', 'java')
if is_windows():
    JAVA_CMD = JAVA_CMD + '.exe'
STORM_THRIFT_TRANSPORT_KEY = "storm.thrift.transport"
NIMBUS_HOST_KEY = "nimbus.host"
NIMBUS_THRIFT_PORT_KEY = "nimbus.thrift.port"

if not os.path.isfile(JAVA_CMD):
    print "Unable to find "+JAVA_CMD+" please check JAVA_HOME"
    sys.exit(1)

def get_config_opts():
    global CONFIG_OPTS
    return "-Dstorm.options=" + (','.join(CONFIG_OPTS)).replace(' ', "%%%%")

def get_jars_full(adir):
    files = os.listdir(adir)
    ret = []
    for f in files:
        if f.endswith(".jar"):
            ret.append(os.path.join(adir, f))
    return ret

def get_classpath(extrajars):
    ret = (get_jars_full(os.path.join(STORM_DIR ,"lib")))
    ret.extend(extrajars)

    sep = ";" if is_windows() else ":"
    return normclasspath(sep.join(ret))

def print_remoteconfvalue(name):
    """Syntax: [storm-slider --app remoteconfvalue conf-name]

    Prints out the value for conf-name in the cluster's Storm configs.
    This command must be run on a cluster machine.
    """
    storm_conf = storm_conf_values([name])
    for conf in storm_conf:
        print conf

def parse_args(string):
    r"""Takes a string of whitespace-separated tokens and parses it into a list.
    Whitespace inside tokens may be quoted with single quotes, double quotes or
    backslash (similar to command-line arguments in bash).

    >>> parse_args(r'''"a a" 'b b' c\ c "d'd" 'e"e' 'f\'f' "g\"g" "i""i" 'j''j' k" "k l' l' mm n\\n''')
    ['a a', 'b b', 'c c', "d'd", 'e"e', "f'f", 'g"g', 'ii', 'jj', 'k k', 'l l', 'mm', r'n\n']
    """
    re_split = re.compile(r'''((?:
        [^\s"'\\] |
        "(?: [^"\\] | \\.)*" |
        '(?: [^'\\] | \\.)*' |
        \\.
    )+)''', re.VERBOSE)
    args = re_split.split(string)[1::2]
    args = [re.compile(r'"((?:[^"\\]|\\.)*)"').sub('\\1', x) for x in args]
    args = [re.compile(r"'((?:[^'\\]|\\.)*)'").sub('\\1', x) for x in args]
    return [re.compile(r'\\(.)').sub('\\1', x) for x in args]

def exec_storm_class(klass, jvmtype="-server", jvmopts=[], extrajars=[], args=[], fork=False):
    global CONFFILE
    storm_log_dir = os.path.join(STORM_DIR, "logs")
    all_args = [
        "java", jvmtype, get_config_opts(),
        "-Dstorm.home=" + STORM_DIR,
        "-cp", get_classpath(extrajars),
    ] + jvmopts + [klass] + list(args)
    print "Running: " + " ".join(all_args)
    if is_windows():
        sub.call([JAVA_CMD] + all_args[1:])
    else:
        os.execvp(JAVA_CMD, all_args) # replaces the current process and never returns

def jar(jarfile, klass, *args):
    """Syntax: [storm-slider --app jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments.
    The storm jars and configs in ~/.storm are put on the classpath.
    The process is configured so that StormSubmitter
    (http://nathanmarz.github.com/storm/doc/backtype/storm/StormSubmitter.html)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    exec_storm_class(
        klass,
        jvmtype="-client",
        extrajars=[jarfile, USER_CONF_DIR, os.path.join(STORM_DIR ,"bin")],
        args=args,
        jvmopts=JAR_JVM_OPTS + ["-Dstorm.jar=" + jarfile])

def kill(*args):
    """Syntax: [storm-slider --app kill topology-name [-w wait-time-secs]]

    Kills the topology with the name topology-name. Storm will
    first deactivate the topology's spouts for the duration of
    the topology's message timeout to allow all messages currently
    being processed to finish processing. Storm will then shutdown
    the workers and clean up their state. You can override the length
    of time Storm waits between deactivation and shutdown with the -w flag.
    """
    exec_storm_class(
        "backtype.storm.command.kill_topology",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR , "bin")])

def activate(*args):
    """Syntax: [storm-slider --app activate topology-name]

    Activates the specified topology's spouts.
    """
    exec_storm_class(
        "backtype.storm.command.activate",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR , "bin")])

def listtopos(*args):
    """Syntax: [storm-slider --app list]

    List the running topologies and their statuses.
    """
    exec_storm_class(
        "backtype.storm.command.list",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR , "bin")])

def deactivate(*args):
    """Syntax: [storm-slider --app deactivate topology-name]

    Deactivates the specified topology's spouts.
    """
    exec_storm_class(
        "backtype.storm.command.deactivate",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR , "bin")])

def rebalance(*args):
    """Syntax: [storm-slider --app rebalance topology-name [-w wait-time-secs] [-n new-num-workers] [-e component=parallelism]*]

    Sometimes you may wish to spread out where the workers for a topology
    are running. For example, let's say you have a 10 node cluster running
    4 workers per node, and then let's say you add another 10 nodes to
    the cluster. You may wish to have Storm spread out the workers for the
    running topology so that each node runs 2 workers. One way to do this
    is to kill the topology and resubmit it, but Storm provides a "rebalance"
    command that provides an easier way to do this.

    Rebalance will first deactivate the topology for the duration of the
    message timeout (overridable with the -w flag) and then redistribute
    the workers evenly around the cluster. The topology will then return to
    its previous state of activation (so a deactivated topology will still
    be deactivated and an activated topology will go back to being activated).

    The rebalance command can also be used to change the parallelism of a running topology.
    Use the -n and -e switches to change the number of workers or number of executors of a component
    respectively.
    """
    exec_storm_class(
        "backtype.storm.command.rebalance",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, os.path.join(STORM_DIR,"bin")])

def version():
    """Syntax: [storm-slider --app version]
    Prints the version number of this Storm release.
    """
    releasefile = os.path.join(STORM_DIR, "RELEASE")
    if os.path.exists(releasefile):
        print open(releasefile).readline().strip()
    else:
        print "Unknown"

def quicklinks():
    """Syntax: [storm-slider --app  version]
    Prints the quicklinks information of storm-slider registry"
    """
    global CMD_OPTS
    all_args = ["slider", "registry", "--getconf", "quicklinks","--format", "json", "--name"]
    if 'app_name' in CMD_OPTS.keys():
        all_args.append(CMD_OPTS['app_name'])
    else:
        print_usage()
        sys.exit(1)

    if 'user' in CMD_OPTS.keys():
        all_args.append( "--user "+CMD_OPTS['user'])

    #os.spawnvp(os.P_WAIT,SLIDER_CMD, all_args)
    cmd = [SLIDER_CMD] + all_args[1:]
    if is_windows():
        cmd = ['python'] + cmd

    sub.call(cmd)

def get_storm_config_json():
    global CMD_OPTS
    all_args = ["slider.py", "registry", "--getconf", "storm-site","--format", "json", "--dest", CONFFILE, "--name"]
    if 'app_name' in CMD_OPTS.keys():
       all_args.append(CMD_OPTS['app_name'])
    else:
        print_usage()
        sys.exit(1)

    if 'user' in CMD_OPTS.keys():
        all_args.append("--user")
        all_args.append(CMD_OPTS['user'])

    #os.spawnvp(os.P_WAIT,SLIDER_CMD, all_args)
    cmd = [SLIDER_CMD] + all_args[1:]

    if is_windows():
        cmd = ['python'] + cmd

    sub.call(cmd)
    if not os.path.exists(CONFFILE):
        print "Failed to read slider deployed storm config"
        sys.exit(1)

def storm_conf_values(keys):
    file = open(CONFFILE,"r")
    data = json.load(file)
    storm_args = []
    for key in keys:
        if data.has_key(key):
            storm_args.append(key+"="+data[key])
    return storm_args

def print_commands():
    """Print all client commands and link to documentation"""
    print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
    print "\nHelp:", "\n\thelp", "\n\thelp <command>"

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if COMMANDS.has_key(command):
            print (COMMANDS[command].__doc__ or
                   "No documentation provided for <%s>" % command)
        else:
            print "<%s> is not a valid command" % command
    else:
        print "Please provide yarn app name followed by command"
        print "storm-slider --app --user"
        print_commands()


def unknown_command(*args):
    print "Unknown command: [storm-slider %s]" % ' '.join(sys.argv[1:])
    print_usage()

COMMANDS = {"jar": jar, "kill": kill, "remoteconfvalue": print_remoteconfvalue,
            "activate": activate, "deactivate": deactivate, "rebalance": rebalance, "help": print_usage,
            "list": listtopos, "version": version, "quicklinks" : quicklinks}

def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)

def parse_config_opts(args):
    curr = args[:]
    curr.reverse()
    global CMD_OPTS
    global CONFIG_OPTS
    args_list = []
    while len(curr) > 0:
        token = curr.pop()
        if token == "--app":
            CMD_OPTS['app_name'] = curr.pop() if (len(curr) != 0) else None
        elif token == "--user":
            CMD_OPTS['user'] =  curr.pop() if (len(curr) != 0) else None
        elif token == "-c" and len(curr) != 0:
            CONFIG_OPTS.append(curr.pop())
        else:
            args_list.append(token)
    return args_list

def main():
    args = parse_config_opts(sys.argv[1:])
    if len(args) < 1:
        print_usage()
        sys.exit(-1)
    COMMAND = args[0]
    ARGS = args[1:]
    if (COMMAND != 'help'):
        get_storm_config_json()
        storm_conf = storm_conf_values([NIMBUS_HOST_KEY,NIMBUS_THRIFT_PORT_KEY,STORM_THRIFT_TRANSPORT_KEY])
        parse_config(storm_conf)
    try:
        (COMMANDS.get(COMMAND, unknown_command))(*ARGS)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise
    finally:
        if (os.path.isfile(CONFFILE)):
            os.remove(CONFFILE)

if __name__ == "__main__":
    main()
