#!/usr/bin/env python

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


import argparse
import json
import os
import shlex
import subprocess
import sys
from random import randint

from argparse import HelpFormatter
from operator import attrgetter

class SortingHelpFormatter(HelpFormatter):

    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter('option_strings'))
        super(SortingHelpFormatter, self).add_arguments(actions)

if sys.version_info[0] == 2:
    import ConfigParser as configparser
    from urllib import quote_plus
else:
    import configparser
    from urllib.parse import quote_plus

def is_windows():
    return sys.platform.startswith('win')

def identity(x):
    return x

def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, errors = p.communicate()
    lines = output.split(os.linesep)
    return lines[0]

def get_config_opts(storm_config_opts):
    return "-Dstorm.options=" + ','.join([quote_plus(s) for s in storm_config_opts])

def get_jars_full(adir):
    files = []
    if os.path.isdir(adir):
        files = os.listdir(adir)
    elif os.path.exists(adir):
        files = [adir]

    return [os.path.join(adir, f) for f in files if f.endswith(".jar")]

# If given path is a dir, make it a wildcard so the JVM will include all JARs in the directory.
def get_wildcard_dir(path):
    ret = []
    if os.path.isdir(path):
        ret = [(os.path.join(path, "*"))]
    elif os.path.exists(path):
        ret = [path]
    return ret

def get_java_cmd():
    cmd = 'java' if not is_windows() else 'java.exe'
    if JAVA_HOME:
        cmd = os.path.join(JAVA_HOME, 'bin', cmd)
    return cmd

def confvalue(name, storm_config_opts, extrapaths, overriding_conf_file=None, daemon=True):
    command = [
        JAVA_CMD, "-client", get_config_opts(storm_config_opts),
        "-Dstorm.conf.file=" + (overriding_conf_file if overriding_conf_file else ""),
        "-cp", get_classpath(extrajars=extrapaths, daemon=daemon), "org.apache.storm.command.ConfigValue", name
    ]
    output = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]
    # python 3
    if not isinstance(output, str):
        output = output.decode('utf-8')
    lines = output.split(os.linesep)
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    return ""


def get_classpath(extrajars, daemon=True, client=False):
    ret = get_wildcard_dir(STORM_DIR)
    if client:
        ret.extend(get_wildcard_dir(STORM_WORKER_LIB_DIR))
    else :
        ret.extend(get_wildcard_dir(STORM_LIB_DIR))
    ret.extend(get_wildcard_dir(os.path.join(STORM_DIR, "extlib")))
    if daemon:
        ret.extend(get_wildcard_dir(os.path.join(STORM_DIR, "extlib-daemon")))
    if STORM_EXT_CLASSPATH:
        ret.append(STORM_EXT_CLASSPATH)
    if daemon and STORM_EXT_CLASSPATH_DAEMON:
        ret.append(STORM_EXT_CLASSPATH_DAEMON)
    ret.extend(extrajars)
    return NORMAL_CLASS_PATH(os.pathsep.join(ret))


def init_storm_env():

    global NORMAL_CLASS_PATH, STORM_DIR, USER_CONF_DIR, STORM_CONF_DIR, STORM_WORKER_LIB_DIR, STORM_LIB_DIR,\
        STORM_TOOLS_LIB_DIR, STORM_WEBAPP_LIB_DIR, STORM_BIN_DIR, STORM_LOG4J2_CONF_DIR, STORM_SUPERVISOR_LOG_FILE,\
        CLUSTER_CONF_DIR, JAR_JVM_OPTS, JAVA_HOME, JAVA_CMD, CONF_FILE, STORM_EXT_CLASSPATH, \
        STORM_EXT_CLASSPATH_DAEMON, LOCAL_TTL_DEFAULT

    NORMAL_CLASS_PATH = cygpath if sys.platform == 'cygwin' else identity
    STORM_DIR = os.sep.join(os.path.realpath( __file__ ).split(os.sep)[:-2])
    USER_CONF_DIR = os.path.expanduser("~" + os.sep + ".storm")
    STORM_CONF_DIR = os.getenv('STORM_CONF_DIR', None)

    CLUSTER_CONF_DIR = STORM_CONF_DIR if STORM_CONF_DIR else os.path.join(STORM_DIR, "conf")

    if (not os.path.isfile(os.path.join(USER_CONF_DIR, "storm.yaml"))):
        USER_CONF_DIR = CLUSTER_CONF_DIR

    STORM_WORKER_LIB_DIR = os.path.join(STORM_DIR, "lib-worker")
    STORM_LIB_DIR = os.path.join(STORM_DIR, "lib")

    STORM_TOOLS_LIB_DIR = os.path.join(STORM_DIR, "lib-tools")
    STORM_WEBAPP_LIB_DIR = os.path.join(STORM_DIR, "lib-webapp")
    STORM_BIN_DIR = os.path.join(STORM_DIR, "bin")
    STORM_LOG4J2_CONF_DIR = os.path.join(STORM_DIR, "log4j2")
    STORM_SUPERVISOR_LOG_FILE = os.getenv('STORM_SUPERVISOR_LOG_FILE', "supervisor.log")

    CONF_FILE = ""
    JAR_JVM_OPTS = shlex.split(os.getenv('STORM_JAR_JVM_OPTS', ''))
    JAVA_HOME = os.getenv('JAVA_HOME', None)
    JAVA_CMD = get_java_cmd()

    if JAVA_HOME and not os.path.exists(JAVA_CMD):
        print("ERROR:  JAVA_HOME is invalid.  Could not find bin/java at %s." % JAVA_HOME)
        sys.exit(1)

    if not os.path.exists(STORM_LIB_DIR):
        print("*" * 20)
        print('''The storm client can only be run from within a release. 
You appear to be trying to run the client from a checkout of Storm's source code.
You can download a Storm release at https://storm.apache.org/downloads.html")''')
        print("*" * 20)
        sys.exit(1)


    STORM_EXT_CLASSPATH = os.getenv('STORM_EXT_CLASSPATH', None)
    STORM_EXT_CLASSPATH_DAEMON = os.getenv('STORM_EXT_CLASSPATH_DAEMON', None)
    LOCAL_TTL_DEFAULT = "20"

    ini_file = os.path.join(CLUSTER_CONF_DIR, 'storm_env.ini')
    if not os.path.isfile(ini_file):
        return
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(ini_file)
    options = config.options('environment')
    for option in options:
        value = config.get('environment', option)
        os.environ[option] = value


def resolve_dependencies(artifacts, artifact_repositories, maven_local_repos_dir,
                         proxy_url, proxy_username, proxy_password):
    if not artifacts:
        return {}

    print("Resolving dependencies on demand: artifacts (%s) with repositories (%s)" % (artifacts, artifact_repositories))

    if maven_local_repos_dir:
        print("Local repository directory: %s" % maven_local_repos_dir)

    if proxy_url:
        print("Proxy information: url (%s) username (%s)" % (proxy_url, proxy_username))

    sys.stdout.flush()

    # storm-submit module doesn't rely on storm-core and relevant libs
    extrajars = get_wildcard_dir(os.path.join(STORM_TOOLS_LIB_DIR, "submit-tools"))
    classpath = NORMAL_CLASS_PATH(os.pathsep.join(extrajars))

    command = [
        JAVA_CMD, "-client", "-cp", classpath, "org.apache.storm.submit.command.DependencyResolverMain"
    ]

    command.extend(["--artifacts", artifacts])
    command.extend(["--artifactRepositories", artifact_repositories])

    if maven_local_repos_dir is not None:
        command.extend(["--mavenLocalRepositoryDirectory", maven_local_repos_dir])

    if proxy_url:
        command.extend(["--proxyUrl", proxy_url])
        if proxy_username:
            command.extend(["--proxyUsername", proxy_username])
            command.extend(["--proxyPassword", proxy_password])

    p = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, errors = p.communicate()
    if p.returncode != 0:
        raise RuntimeError("dependency handler returns non-zero code: code<%s> syserr<%s>" % (p.returncode, errors))

    # python 3
    if not isinstance(output, str):
        output = output.decode('utf-8')

    # For debug purpose, uncomment when you need to debug DependencyResolver
    # print("Resolved dependencies: %s" % output)

    try:
        out_dict = json.loads(output)
        return out_dict
    except:
        raise RuntimeError("dependency handler returns non-json response: sysout<%s>", output)


def exec_storm_class(klass, storm_config_opts, jvmtype="-server", jvmopts=[],
                     extrajars=[], main_class_args=[], fork=False, daemon=True, client=False, daemonName="",
                     overriding_conf_file=None):
    storm_log_dir = confvalue("storm.log.dir", storm_config_opts=storm_config_opts,
                              extrapaths=[CLUSTER_CONF_DIR], overriding_conf_file=overriding_conf_file)
    if storm_log_dir is None or storm_log_dir in ["null", ""]:
        storm_log_dir = os.path.join(STORM_DIR, "logs")
    all_args = [
        JAVA_CMD, jvmtype,
        "-Ddaemon.name=" + daemonName,
        get_config_opts(storm_config_opts),
       "-Dstorm.home=" + STORM_DIR,
       "-Dstorm.log.dir=" + storm_log_dir,
       "-Djava.library.path=" + confvalue("java.library.path", storm_config_opts, extrajars, daemon=daemon),
       "-Dstorm.conf.file=" + (overriding_conf_file if overriding_conf_file else ""),
       "-cp", get_classpath(extrajars, daemon, client=client),
    ] + jvmopts + [klass] + list(main_class_args)
    print("Running: " + " ".join(all_args))
    sys.stdout.flush()
    exit_code = 0
    if fork:
        exit_code = os.spawnvp(os.P_WAIT, JAVA_CMD, all_args)
    elif is_windows():
        # handling whitespaces in JAVA_CMD
        try:
            ret = subprocess.check_output(all_args, stderr=subprocess.STDOUT)
            print(ret)
        except subprocess.CalledProcessError as e:
            print(e.output)
            sys.exit(e.returncode)
    else:
        os.execvp(JAVA_CMD, all_args)
    return exit_code


def run_client_jar(klass, args, daemon=False, client=True, extrajvmopts=[]):
    local_jars = args.jars.split(",")
    jarfile = args.topology_jar_path

    artifact_to_file_jars = resolve_dependencies(
        args.artifacts, args.artifactRepositories,
        args.mavenLocalRepositoryDirectory, args.proxyUrl,
        args.proxyUsername, args.proxyPassword
    )

    extra_jars = [jarfile, USER_CONF_DIR, STORM_BIN_DIR]
    extra_jars.extend(local_jars)
    extra_jars.extend(artifact_to_file_jars.values())
    exec_storm_class(
        klass, args.storm_config_opts,
        jvmtype="-client",
        extrajars=extra_jars,
        main_class_args=args.main_args,
        daemon=daemon,
        client=client,
        jvmopts=JAR_JVM_OPTS + extrajvmopts + ["-Dstorm.jar=" + jarfile] +
                ["-Dstorm.dependency.jars=" + ",".join(local_jars)] +
                ["-Dstorm.dependency.artifacts=" + json.dumps(artifact_to_file_jars)],
        overriding_conf_file=args.config)


def print_localconfvalue(args):
    print(args.conf_name + ": " + confvalue(args.conf_name, args.storm_config_opts,
                                            [USER_CONF_DIR], overriding_conf_file=args.config))


def print_remoteconfvalue(args):
    print(args.conf_name + ": " + confvalue(args.conf_name, args.storm_config_opts,
                                            [CLUSTER_CONF_DIR], overriding_conf_file=args.config))


def initialize_main_command():
    main_parser = argparse.ArgumentParser(prog="storm", formatter_class=SortingHelpFormatter)

    subparsers = main_parser.add_subparsers(help="")

    initialize_jar_subcommand(subparsers)
    initialize_localconfvalue_subcommand(subparsers)
    initialize_remoteconfvalue_subcommand(subparsers)
    initialize_local_subcommand(subparsers)
    initialize_sql_subcommand(subparsers)
    initialize_kill_subcommand(subparsers)
    initialize_upload_credentials_subcommand(subparsers)
    initialize_blobstore_subcommand(subparsers)
    initialize_heartbeats_subcommand(subparsers)
    initialize_activate_subcommand(subparsers)
    initialize_set_log_level_subcommand(subparsers)
    initialize_listtopos_subcommand(subparsers)
    initialize_deactivate_subcommand(subparsers)
    initialize_rebalance_subcommand(subparsers)
    initialize_get_errors_subcommand(subparsers)
    initialize_healthcheck_subcommand(subparsers)
    initialize_kill_workers_subcommand(subparsers)
    initialize_admin_subcommand(subparsers)
    initialize_shell_subcommand(subparsers)
    initialize_repl_subcommand(subparsers)
    initialize_nimbus_subcommand(subparsers)
    initialize_pacemaker_subcommand(subparsers)
    initialize_supervisor_subcommand(subparsers)
    initialize_ui_subcommand(subparsers)
    initialize_logviewer_subcommand(subparsers)
    initialize_drpc_client_subcommand(subparsers)
    initialize_drpc_subcommand(subparsers)
    initialize_dev_zookeeper_subcommand(subparsers)
    initialize_version_subcommand(subparsers)
    initialize_classpath_subcommand(subparsers)
    initialize_server_classpath_subcommand(subparsers)
    initialize_monitor_subcommand(subparsers)

    return main_parser


def initialize_localconfvalue_subcommand(subparsers):
    command_help = '''Prints out the value for conf-name in the local Storm configs.
    The local Storm configs are the ones in ~/.storm/storm.yaml merged
    in with the configs in defaults.yaml.'''

    sub_parser = subparsers.add_parser("localconfvalue", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.add_argument("conf_name")
    sub_parser.set_defaults(func=print_localconfvalue)
    add_common_options(sub_parser)



def initialize_remoteconfvalue_subcommand(subparsers):
    command_help = '''Prints out the value for conf-name in the cluster's Storm configs.
    The cluster's Storm configs are the ones in $STORM-PATH/conf/storm.yaml
    merged in with the configs in defaults.yaml.

    This command must be run on a cluster machine.'''

    sub_parser = subparsers.add_parser("remoteconfvalue", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.add_argument("conf_name")
    sub_parser.set_defaults(func=print_remoteconfvalue)
    add_common_options(sub_parser)


def add_common_options(parser, main_args=True):
    parser.add_argument("--config", default=None, help="Override default storm conf file")
    parser.add_argument(
        "-storm_config_opts", "-c", action="append", default=[],
        help="Override storm conf properties , e.g. nimbus.ui.port=4443"
    )
    if main_args:
        parser.add_argument(
            "main_args", metavar="main_args",
            nargs='*', help="Runs the main method with the specified arguments."
        )

def remove_common_options(sys_args):
    flags_to_filter = ["-c", "-storm_config_opts", "--config"]
    filtered_sys_args = [
        sys_args[i] for i in range(0, len(sys_args)) if (not (sys_args[i] in flags_to_filter) and ((i<1) or
                                         not (sys_args[i - 1] in flags_to_filter)))
    ]
    return filtered_sys_args

def add_topology_jar_options(parser):
    parser.add_argument(
        "topology_jar_path", metavar="topology-jar-path",
        help="will upload the jar at topology-jar-path when the topology is submitted."
    )
    parser.add_argument(
        "topology_main_class", metavar="topology-main-class",
    help="main class of the topology jar being submitted"
    )


def add_client_jar_options(parser):

    parser.add_argument("--jars", help='''
    When you want to ship other jars which are not included to application jar, you can pass them to --jars option with comma-separated string.
    For example, --jars "your-local-jar.jar,your-local-jar2.jar" will load your-local-jar.jar and your-local-jar2.jar.
    ''', default="")

    parser.add_argument("--artifacts", help='''
     When you want to ship maven artifacts and its transitive dependencies, you can pass them to --artifacts with comma-separated string.
    You can also exclude some dependencies like what you're doing in maven pom.
    Please add exclusion artifacts with '^' separated string after the artifact.
    For example, -artifacts "redis.clients:jedis:2.9.0,org.apache.kafka:kafka-clients:1.0.0^org.slf4j:slf4j-api" will load jedis and kafka-clients artifact and all of transitive dependencies but exclude slf4j-api from kafka.
        ''', default="")


    parser.add_argument("--artifactRepositories", help='''
    When you need to pull the artifacts from other than Maven Central, you can pass remote repositories to --artifactRepositories option with a comma-separated string.
    Repository format is "<name>^<url>". '^' is taken as separator because URL allows various characters.
    For example, --artifactRepositories "jboss-repository^http://repository.jboss.com/maven2,HDPRepo^http://repo.hortonworks.com/content/groups/public/" will add JBoss and HDP repositories for dependency resolver.
    ''', default="")

    parser.add_argument("--mavenLocalRepositoryDirectory", help="You can provide local maven repository directory via --mavenLocalRepositoryDirectory if you would like to use specific directory. It might help when you don't have '.m2/repository' directory in home directory, because CWD is sometimes non-deterministic (fragile).", default="")


    parser.add_argument("--proxyUrl", help="You can also provide proxy information to let dependency resolver utilizing proxy if needed. URL representation of proxy ('http://host:port')", default="")
    parser.add_argument("--proxyUsername", help="username of proxy if it requires basic auth", default="")
    parser.add_argument("--proxyPassword", help="password of proxy if it requires basic auth", default="")


def initialize_jar_subcommand(subparsers):
    jar_help = """Runs the main method of class with the specified arguments.
    The storm worker dependencies and configs in ~/.storm are put on the classpath.
    The process is configured so that StormSubmitter
    (https://storm.apache.org/releases/current/javadocs/org/apache/storm/StormSubmitter.html)
    will upload the jar at topology-jar-path when the topology is submitted.

    When you pass jars and/or artifacts options, StormSubmitter will upload them when the topology is submitted, and they will be included to classpath of both the process which runs the class, and also workers for that topology.
    """
    jar_parser = subparsers.add_parser("jar", help=jar_help, formatter_class=SortingHelpFormatter)

    add_topology_jar_options(jar_parser)
    add_client_jar_options(jar_parser)

    jar_parser.add_argument(
        "--storm-server-classpath",
        action='store_true',
        help='''
        If for some reason you need to have the full storm classpath,
        not just the one for the worker you may include the command line option `--storm-server-classpath`.
        Please be careful because this will add things to the classpath
        that will not be on the worker classpath
        and could result in the worker not running.'''
    )

    jar_parser.set_defaults(func=jar)
    add_common_options(jar_parser)


def initialize_local_subcommand(subparsers):
    command_help = """Runs the main method of class with the specified arguments but pointing to a local cluster
    The storm jars and configs in ~/.storm are put on the classpath.
    The process is configured so that StormSubmitter
    (https://storm.apache.org/releases/current/javadocs/org/apache/storm/StormSubmitter.html)
    and others will interact with a local cluster instead of the one configured by default.

    Most options should work just like with the storm jar command.
    """
    sub_parser = subparsers.add_parser("local", help=command_help, formatter_class=SortingHelpFormatter)

    add_topology_jar_options(sub_parser)
    add_client_jar_options(sub_parser)

    sub_parser.add_argument(
        "--local-ttl",
        help="sets the number of seconds the local cluster will run for before it shuts down",
        default=LOCAL_TTL_DEFAULT
    )

    sub_parser.add_argument(
        "--java-debug",
        help="lets you turn on java debugging and set the parameters passed to -agentlib:jdwp on the JDK" +
             "e.g transport=dt_socket,address=localhost:8000 will open up a debugging server on port 8000",
        default=None
    )

    sub_parser.add_argument(
        "--local-zookeeper",
        help="""if using an external zookeeper sets the connection string to use for it.""",
        default=None
    )

    sub_parser.set_defaults(func=local)
    add_common_options(sub_parser)


def initialize_kill_subcommand(subparsers):
    command_help = """Kills the topology with the name topology-name. Storm will
    first deactivate the topology's spouts for the duration of
    the topology's message timeout to allow all messages currently
    being processed to finish processing. Storm will then shutdown
    the workers and clean up their state.
    """
    sub_parser = subparsers.add_parser("kill", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.add_argument("topology-name")

    sub_parser.add_argument(
        "-w", "--wait-time-secs",
        help="""override the length of time Storm waits between deactivation and shutdown""",
        default=None, type=check_non_negative
    )

    sub_parser.set_defaults(func=kill)
    add_common_options(sub_parser)


def check_non_negative(value):
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("%s is not a non-zero integer" % value)
    return ivalue

def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is not a positive integer" % value)
    return ivalue

def initialize_upload_credentials_subcommand(subparsers):
    command_help = """Uploads a new set of credentials to a running topology."""
    sub_parser = subparsers.add_parser("upload-credentials", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.add_argument("topology-name")

    sub_parser.add_argument(
        "-f", "--file", default=None,
        help="""provide a properties file with credentials in it to be uploaded"""
    )

    sub_parser.add_argument(
        "-u", "--user", default=None,
        help="""name of the owner of the topology (security precaution)"""
    )

    # If set, this flag will become true meaning that user expects non-empty creds to be uploaded.
    # Command exits with non-zero code if uploaded creds collection is empty.
    sub_parser.add_argument(
        "-e", "--exception-when-empty", action='store_true',
        help="""If specified, throw exception if there are no credentials uploaded. 
                Otherwise, it is default to be false"""
    )

    sub_parser.add_argument(
        "cred_list", nargs='*', help="List of credkeys and their values [credkey credvalue]*"
    )

    sub_parser.set_defaults(func=upload_credentials)
    add_common_options(sub_parser)


def initialize_sql_subcommand(subparsers):
    command_help = """Compiles the SQL statements into a Trident topology and submits it to Storm.
    If user activates explain mode, SQL Runner analyzes each query statement
    and shows query plan instead of submitting topology.
    """

    sub_parser = subparsers.add_parser("sql", help=command_help, formatter_class=SortingHelpFormatter)

    add_client_jar_options(sub_parser)


    sub_parser.add_argument("sql_file", metavar="sql-file")

    group = sub_parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "topology_name", metavar="topology-name",nargs='?'
    )

    group.add_argument("--explain", action="store_true", help="activate explain mode")

    sub_parser.set_defaults(func=sql)
    add_common_options(sub_parser, main_args=False)


def initialize_blobstore_subcommand(subparsers):
    sub_parser = subparsers.add_parser("blobstore", formatter_class=SortingHelpFormatter)
    command_help = """
    For example, the following would create a mytopo:data.tgz key using the data
    stored in data.tgz.  User alice would have full access, bob would have
    read/write access and everyone else would have read access.
    storm blobstore create mytopo:data.tgz -f data.tgz -a u:alice:rwa,u:bob:rw,o::r
    """
    sub_sub_parsers = sub_parser.add_subparsers(help=command_help)

    list_parser = sub_sub_parsers.add_parser(
        "list", help="lists blobs currently in the blob store", formatter_class=SortingHelpFormatter
    )
    list_parser.add_argument(
        "keys", nargs='*')
    add_common_options(list_parser, main_args=False)

    cat_parser = sub_sub_parsers.add_parser(
        "cat", help="read a blob and then either write it to a file, or STDOUT (requires read access).", formatter_class=SortingHelpFormatter
    )
    cat_parser.add_argument("KEY")
    cat_parser.add_argument("-f", '--FILE', default=None)
    add_common_options(cat_parser)


    create_parser = sub_sub_parsers.add_parser(
        "create", help="create a new blob. Contents comes from a FILE or STDIN", formatter_class=SortingHelpFormatter
    )
    create_parser.add_argument("KEY")
    create_parser.add_argument("-f", '--file', default=None)
    create_parser.add_argument(
        "-a", '--acl', default=None,
        help="ACL is in the form [uo]:[username]:[r-][w-][a-] can be comma separated list."
    )
    create_parser.add_argument("-r", "--replication-factor", default=None, type=check_positive)
    add_common_options(create_parser)

    update_parser = sub_sub_parsers.add_parser(
        "update", help="update the contents of a blob.  Contents comes from a FILE or STDIN (requires write access).", formatter_class=SortingHelpFormatter,
    )
    update_parser.add_argument("KEY")
    update_parser.add_argument("-f", '--FILE', default=None)
    add_common_options(update_parser)

    delete_parser = sub_sub_parsers.add_parser(
        "delete", help="delete an entry from the blob store (requires write access).", formatter_class=SortingHelpFormatter
    )
    delete_parser.add_argument("KEY")
    add_common_options(delete_parser)

    set_acl_parser = sub_sub_parsers.add_parser(
        "set-acl", help="set acls for the given key", formatter_class=SortingHelpFormatter
    )
    set_acl_parser.add_argument("KEY")
    set_acl_parser.add_argument(
        "-s", '--set', default=None,
        help="""ACL is in the form [uo]:[username]:[r-][w-][a-] 
        can be comma separated list (requires admin access)."""
    )
    add_common_options(set_acl_parser)

    replication_parser = sub_sub_parsers.add_parser(
        "replication", formatter_class=SortingHelpFormatter
    )
    replication_parser.add_argument("KEY")
    replication_parser.add_argument(
        "--read", action="store_true", help="Used to read the replication factor of the blob",
        default=None
    )
    replication_parser.add_argument(
        "--update", action="store_true", help=" It is used to update the replication factor of a blob.",
        default=None
    )
    replication_parser.add_argument("-r", "--replication-factor", default=None, type=check_positive)
    add_common_options(replication_parser)

    sub_parser.set_defaults(func=blob)
    add_common_options(sub_parser)


def initialize_heartbeats_subcommand(subparsers):
    sub_parser = subparsers.add_parser("heartbeats")
    sub_sub_parsers = sub_parser.add_subparsers()

    list_parser = sub_sub_parsers.add_parser(
        "PATH", help="lists heartbeats nodes under PATH currently in the ClusterState", formatter_class=SortingHelpFormatter
    )
    list_parser.add_argument("PATH")

    get_parser = sub_sub_parsers.add_parser(
        "get", help="Get the heartbeat data at PATH", formatter_class=SortingHelpFormatter
    )
    get_parser.add_argument("PATH")
    sub_parser.set_defaults(func=heartbeats)
    add_common_options(sub_parser)


def initialize_activate_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "activate", help="Activates the specified topology's spouts.", formatter_class=SortingHelpFormatter
    )

    sub_parser.add_argument("topology-name")

    sub_parser.set_defaults(func=activate)
    add_common_options(sub_parser)


def initialize_listtopos_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "list", help="List the running topologies and their statuses.", formatter_class=SortingHelpFormatter
    )

    sub_parser.set_defaults(func=listtopos)
    add_common_options(sub_parser)


def initialize_set_log_level_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "set_log_level", help="""
        Dynamically change topology log levels
        e.g.
        ./bin/storm set_log_level -l ROOT=DEBUG:30 topology-name

        Set the root logger's level to DEBUG for 30 seconds

        ./bin/storm set_log_level -l com.myapp=WARN topology-name

        Set the com.myapp logger's level to WARN for 30 seconds

        ./bin/storm set_log_level -l com.myapp=WARN -l com.myOtherLogger=ERROR:123 topology-name

        Set the com.myapp logger's level to WARN indefinitely, and com.myOtherLogger
        to ERROR for 123 seconds

        ./bin/storm set_log_level -r com.myOtherLogger topology-name

        Clears settings, resetting back to the original level
        """, formatter_class=SortingHelpFormatter
    )

    sub_parser.add_argument("-l", action="append", default=[], help="""
    -l [logger name]=[log level][:optional timeout] where log level is one of:
        ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
    """)
    sub_parser.add_argument("-r", action="append", default=[], help="""
    -r [logger name]
    """)
    sub_parser.add_argument("topology-name")

    sub_parser.set_defaults(func=set_log_level)
    add_common_options(sub_parser)


def initialize_deactivate_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "deactivate", help="Deactivates the specified topology's spouts.", formatter_class=SortingHelpFormatter
    )

    sub_parser.add_argument("topology-name")

    sub_parser.set_defaults(func=deactivate)
    add_common_options(sub_parser)


def initialize_rebalance_subcommand(subparsers):
    command_help = """
    Sometimes you may wish to spread out the workers for a running topology.
    For example, let's say you have a 10 node cluster running
    4 workers per node, and then let's say you add another 10 nodes to
    the cluster. You may wish to have Storm spread out the workers for the
    running topology so that each node runs 2 workers. One way to do this
    is to kill the topology and resubmit it, but Storm provides a "rebalance"
    command that provides an easier way to do this.

    Rebalance will first deactivate the topology for the duration of the
    message timeout (overridable with the -w flag) make requested adjustments to the topology
    and let the scheduler try to find a better scheduling based off of the
    new situation. The topology will then return to its previous state of activation
    (so a deactivated topology will still be deactivated and an activated
    topology will go back to being activated).
    """
    sub_parser = subparsers.add_parser(
        "rebalance", help=command_help, formatter_class=SortingHelpFormatter
    )

    sub_parser.add_argument(
        "-w", "--wait-time-secs",
        help="time to wait before starting to rebalance",
        default=None, type=check_non_negative
    )

    sub_parser.add_argument(
        "-n", "--num-workers", default=None,
        help="change the number of requested workers", type=check_positive
    )

    sub_parser.add_argument(
        "-e", "--executor", action="append", default=[],
        help="change the number of executors for a given component e.g. --executor component_name=6"
    )

    sub_parser.add_argument(
        "-r", "--resources", default=None,
        help="""
        change the resources each component is requesting as used by the resource aware scheduler
        e.g '{"component1": {"resource1": new_amount, "resource2": new_amount, ... }*}'
        """
    )

    sub_parser.add_argument(
        "-t", "--topology-conf", default=None,
        help="change the topology conf"
    )

    sub_parser.add_argument("topology-name")

    sub_parser.set_defaults(func=rebalance)
    add_common_options(sub_parser)


def initialize_get_errors_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "get-errors", help="""Get the latest error from the running topology. The returned result contains
    the key value pairs for component-name and component-error for the components in error.
    The result is returned in json format.""", formatter_class=SortingHelpFormatter
    )

    sub_parser.add_argument("topology-name")

    sub_parser.set_defaults(func=get_errors)
    add_common_options(sub_parser)


def initialize_healthcheck_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "node-health-check", help="""Run health checks on the local supervisor.""", formatter_class=SortingHelpFormatter
    )

    sub_parser.set_defaults(func=healthcheck)
    add_common_options(sub_parser)


def initialize_kill_workers_subcommand(subparsers):
    sub_parser = subparsers.add_parser(
        "kill_workers", help="""Kill the workers running on this supervisor. This command should be run
    on a supervisor node. If the cluster is running in secure mode, then user needs
    to have admin rights on the node to be able to successfully kill all workers.""", formatter_class=SortingHelpFormatter
    )

    sub_parser.set_defaults(func=kill_workers)
    add_common_options(sub_parser)


def initialize_admin_subcommand(subparsers):
    sub_parser = subparsers.add_parser("admin", help="""The storm admin command provides access to several operations that can help
    an administrator debug or fix a cluster.""", formatter_class=SortingHelpFormatter)
    sub_sub_parsers = sub_parser.add_subparsers()

    remove_sub_sub_parser = sub_sub_parsers.add_parser(
        "remove_corrupt_topologies", help="""This command should be run on a nimbus node as
    the same user nimbus runs as.  It will go directly to zookeeper + blobstore
    and find topologies that appear to be corrupted because of missing blobs.
    It will kill those topologies.""", formatter_class=SortingHelpFormatter
    )

    add_common_options(remove_sub_sub_parser)

    zk_cli_parser = sub_sub_parsers.add_parser(
        "zk_cli", help="""This command will launch a zookeeper cli pointing to the
    storm zookeeper instance logged in as the nimbus user.  It should be run on
    a nimbus server as the user nimbus runs as.""", formatter_class=SortingHelpFormatter
    )

    zk_cli_parser.add_argument(
        "-s", "--server", default=None, help="""Set the connection string to use,
        defaults to storm connection string"""
    )

    zk_cli_parser.add_argument(
        "-t", "--time-out", default=None, help="""Set the timeout in seconds to use, defaults to storm
            zookeeper timeout.""", type=check_non_negative
    )

    zk_cli_parser.add_argument(
        "-w", "--write", default=None, action="store_true",
        help="""Allow for writes, defaults to read only, we don't want to
            cause problems."""
    )

    zk_cli_parser.add_argument(
        "-n", "--no-root", default=None, action="store_true",
        help="""Don't include the storm root on the default connection string."""
    )

    zk_cli_parser.add_argument(
        "-j", "--jaas", default=None, help="""Include a jaas file that should be used when
            authenticating with ZK defaults to the
            java.security.auth.login.config conf"""
    )

    add_common_options(zk_cli_parser)

    creds_parser = sub_sub_parsers.add_parser(
        "creds", help="""Print the credential keys for a topology.""", formatter_class=SortingHelpFormatter
    )

    creds_parser.add_argument("topology_id")
    add_common_options(creds_parser)

    sub_parser.set_defaults(func=admin)
    add_common_options(sub_parser)


def initialize_shell_subcommand(subparsers):
    command_help = """
    Archives resources to jar and uploads jar to Nimbus, and executes following arguments on "local". Useful for non JVM languages.
    eg: `storm shell resources/ python topology.py arg1 arg2`"""

    sub_parser = subparsers.add_parser("shell", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.add_argument("resourcesdir")
    sub_parser.add_argument("command")
    sub_parser.add_argument("args", nargs='*', default=[])

    sub_parser.set_defaults(func=shell)
    add_common_options(sub_parser, main_args=False)


def initialize_repl_subcommand(subparsers):
    command_help = """
       DEPRECATED: This subcommand may be removed in a future release.
    Opens up a Clojure REPL with the storm jars and configuration
    on the classpath. Useful for debugging."""
    sub_parser = subparsers.add_parser("repl", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.set_defaults(func=repl)
    add_common_options(sub_parser)


def initialize_nimbus_subcommand(subparsers):
    command_help = """
    Launches the nimbus daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (https://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    sub_parser = subparsers.add_parser("nimbus", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=nimbus)
    add_common_options(sub_parser)


def initialize_pacemaker_subcommand(subparsers):
    command_help = """
    Launches the Pacemaker daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (https://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    sub_parser = subparsers.add_parser("pacemaker", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=pacemaker)
    add_common_options(sub_parser)


def initialize_supervisor_subcommand(subparsers):
    command_help = """
    Launches the supervisor daemon. This command should be run
    under supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (https://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    sub_parser = subparsers.add_parser("supervisor", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=supervisor)
    add_common_options(sub_parser)

def initialize_ui_subcommand(subparsers):
    command_help = """
    Launches the UI daemon. The UI provides a web interface for a Storm
    cluster and shows detailed stats about running topologies. This command
    should be run under supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (https://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    sub_parser = subparsers.add_parser("ui", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=ui)
    add_common_options(sub_parser)


def initialize_logviewer_subcommand(subparsers):
    command_help = """
    Launches the log viewer daemon. It provides a web interface for viewing
    storm log files. This command should be run under supervision with a
    tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (https://storm.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    sub_parser = subparsers.add_parser("logviewer", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=logviewer)
    add_common_options(sub_parser)


def initialize_drpc_client_subcommand(subparsers):
    command_help = """
    Provides a very simple way to send DRPC requests. The server and port are picked from the configs.
    """

    sub_parser = subparsers.add_parser("drpc-client", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.add_argument(
        "-f", "--function", default=None, help="""If the -f argument is supplied to set the function name all of the arguments are treated
    as arguments to the function.  If no function is given the arguments must
    be pairs of function argument."""
    )
    sub_parser.add_argument("function_arguments", nargs='*', default=[])

    sub_parser.set_defaults(func=drpc_client)
    add_common_options(sub_parser, main_args=False)


def initialize_drpc_subcommand(subparsers):
    command_help = """
    Launches a DRPC daemon. This command should be run under supervision
    with a tool like daemontools or monit.

    See Distributed RPC for more information.
    (https://storm.apache.org/documentation/Distributed-RPC)
    """
    sub_parser = subparsers.add_parser("drpc", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=drpc)
    add_common_options(sub_parser)


def initialize_dev_zookeeper_subcommand(subparsers):
    command_help = """
    Launches a fresh Zookeeper server using "dev.zookeeper.path" as its local dir and
    "storm.zookeeper.port" as its port. This is only intended for development/testing, the
    Zookeeper instance launched is not configured to be used in production.
    """
    sub_parser = subparsers.add_parser("dev-zookeeper", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=dev_zookeeper)
    add_common_options(sub_parser)


def initialize_version_subcommand(subparsers):
    command_help = """Prints the version number of this Storm release."""
    sub_parser = subparsers.add_parser("version", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=version)
    add_common_options(sub_parser)


def initialize_classpath_subcommand(subparsers):
    command_help = """Prints the classpath used by the storm client when running commands."""
    sub_parser = subparsers.add_parser("classpath", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=print_classpath)
    add_common_options(sub_parser)


def initialize_server_classpath_subcommand(subparsers):
    command_help = """Prints the classpath used by the storm servers when running commands."""
    sub_parser = subparsers.add_parser("server_classpath", help=command_help, formatter_class=SortingHelpFormatter)
    sub_parser.set_defaults(func=print_server_classpath)
    add_common_options(sub_parser)


def initialize_monitor_subcommand(subparsers):
    command_help = """Monitor given topology's throughput interactively."""
    sub_parser = subparsers.add_parser("monitor", help=command_help, formatter_class=SortingHelpFormatter)

    sub_parser.add_argument("topology-name")
    sub_parser.add_argument(
        "-i", "--interval", type=check_positive, default=None,
        help="""By default, poll-interval is 4 seconds"""
    )
    sub_parser.add_argument("-m", "--component", type=check_positive, default=None)
    sub_parser.add_argument("-s", "--stream", default=None)
    sub_parser.add_argument("-w", "--watch", default=None)
    sub_parser.set_defaults(func=monitor)
    add_common_options(sub_parser)


def jar(args):
    run_client_jar(
        args.topology_main_class, args,
        client=not args.storm_server_classpath, daemon=False)


def local(args):
    extrajvmopts = ["-Dstorm.local.sleeptime=" + args.local_ttl]
    if args.java_debug:
        extrajvmopts += ["-agentlib:jdwp=" + args.java_debug]
    args.main_args = [args.topology_main_class] + args.main_args
    run_client_jar(
        "org.apache.storm.LocalCluster", args,
        client=False, daemon=False, extrajvmopts=extrajvmopts)


def sql(args):
    local_jars = [arg for arg in args.jars.split(",") if arg]

    artifact_to_file_jars = resolve_dependencies(
        args.artifacts, args.artifactRepositories,
        args.mavenLocalRepositoryDirectory, args.proxyUrl,
        args.proxyUsername, args.proxyPassword
    )

    # include storm-sql-runtime jar(s) to local jar list
    # --jars doesn't support wildcard so it should call get_jars_full
    sql_runtime_jars = get_jars_full(os.path.join(STORM_TOOLS_LIB_DIR, "sql", "runtime"))
    local_jars.extend(sql_runtime_jars)

    extra_jars = [USER_CONF_DIR, STORM_BIN_DIR]
    extra_jars.extend(local_jars)
    extra_jars.extend(artifact_to_file_jars.values())

    # include this for running StormSqlRunner, but not for generated topology
    sql_core_jars = get_wildcard_dir(os.path.join(STORM_TOOLS_LIB_DIR, "sql", "core"))
    extra_jars.extend(sql_core_jars)

    if args.explain:
        sql_args = ["--file", args.sql_file, "--explain"]
    else:
        sql_args = ["--file", args.sql_file, "--topology", args.topology_name]

    exec_storm_class(
        "org.apache.storm.sql.StormSqlRunner", storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=extra_jars,
        main_class_args=sql_args,
        daemon=False,
        jvmopts=["-Dstorm.dependency.jars=" + ",".join(local_jars)] +
                ["-Dstorm.dependency.artifacts=" + json.dumps(artifact_to_file_jars)],
        overriding_conf_file=args.config)


def kill(args):
    exec_storm_class(
        "org.apache.storm.command.KillTopology",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def upload_credentials(args):
    if (len(args.cred_list) % 2 != 0):
        raise argparse.ArgumentTypeError("please provide a list of cred key and value pairs " + cred_list)
    exec_storm_class(
        "org.apache.storm.command.UploadCredentials",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def blob(args):
    if hasattr(args, "update") and args.update and not args.replication_factor:
        raise argparse.ArgumentTypeError("Replication factor needed when doing blob update")
    exec_storm_class(
        "org.apache.storm.command.Blobstore",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def heartbeats(args):
    exec_storm_class(
        "org.apache.storm.command.Heartbeats",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def activate(args):
    exec_storm_class(
        "org.apache.storm.command.Activate",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)

def listtopos(args):
    exec_storm_class(
        "org.apache.storm.command.ListTopologies",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)

def set_log_level(args):
    for log_level in args.l:
        try:
            _, new_value = log_level.split("=")
            if ":" in new_value:
                _, timeout = new_value.split(":")
                int(timeout)
        except:
            raise argparse.ArgumentTypeError("Should be in the form[logger name]=[log level][:optional timeout]")
    exec_storm_class(
        "org.apache.storm.command.SetLogLevel",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)

def deactivate(args):
    exec_storm_class(
        "org.apache.storm.command.Deactivate",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def rebalance(args):
    for executor in args.executor:
        try:
            _, new_value = executor.split("=")
            new_value = int(new_value)
            if new_value < 0:
                raise argparse.ArgumentTypeError("Executor count should be > 0")
        except:
            raise argparse.ArgumentTypeError("Should be in the form component_name=new_executor_count")
    exec_storm_class(
        "org.apache.storm.command.Rebalance",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def get_errors(args):
    exec_storm_class(
        "org.apache.storm.command.GetErrors",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def healthcheck(args):
    exec_storm_class(
        "org.apache.storm.command.HealthCheck",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def kill_workers(args):
    exec_storm_class(
        "org.apache.storm.command.KillWorkers",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def admin(args):
    exec_storm_class(
        "org.apache.storm.command.AdminCommands",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def shell(args):
    tmpjarpath = "stormshell" + str(randint(0, 10000000)) + ".jar"
    os.system("jar cf %s %s" % (tmpjarpath, args.resourcesdir))
    runnerargs = [tmpjarpath, args.command]
    runnerargs.extend(args.args)
    exec_storm_class(
        "org.apache.storm.command.ShellSubmission", storm_config_opts=args.storm_config_opts,
        main_class_args=runnerargs,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR],
        fork=True,
        overriding_conf_file=args.config)
    os.system("rm " + tmpjarpath)


def repl(args):
    cppaths = [CLUSTER_CONF_DIR]
    exec_storm_class(
        "clojure.main", storm_config_opts=args.storm_config_opts, jvmtype="-client", extrajars=cppaths,
        overriding_conf_file=args.config
    )


def get_log4j2_conf_dir(storm_config_opts, args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_log4j2_conf_dir = confvalue(
        "storm.log4j2.conf.dir", storm_config_opts=storm_config_opts,
        extrapaths=cppaths, overriding_conf_file=args.config
    )
    if(not storm_log4j2_conf_dir or storm_log4j2_conf_dir == "null"):
        storm_log4j2_conf_dir = STORM_LOG4J2_CONF_DIR
    elif(not os.path.isabs(storm_log4j2_conf_dir)):
        storm_log4j2_conf_dir = os.path.join(STORM_DIR, storm_log4j2_conf_dir)
    return storm_log4j2_conf_dir


def nimbus(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = shlex.split(confvalue(
        "nimbus.childopts", storm_config_opts=storm_config_opts, extrapaths=cppaths, overriding_conf_file=args.config
        )) + [
            "-Djava.deserialization.disabled=true",
            "-Dlogfile.name=nimbus.log",
            "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml"),
        ]
    exec_storm_class(
        "org.apache.storm.daemon.nimbus.Nimbus", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="nimbus",
        extrajars=cppaths,
        jvmopts=jvmopts,
        overriding_conf_file=args.config)


def pacemaker(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)

    jvmopts = shlex.split(confvalue(
        "pacemaker.childopts", storm_config_opts=storm_config_opts,
        extrapaths=cppaths, overriding_conf_file=args.config)
    ) + [
        "-Djava.deserialization.disabled=true",
        "-Dlogfile.name=pacemaker.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml"),
        ]
    exec_storm_class(
        "org.apache.storm.pacemaker.Pacemaker", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="pacemaker",
        extrajars=cppaths,
        jvmopts=jvmopts,
        overriding_conf_file=args.config)


def supervisor(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = shlex.split(confvalue(
        "supervisor.childopts", storm_config_opts=storm_config_opts,
        extrapaths=cppaths, overriding_conf_file=args.config)
    ) + [
        "-Djava.deserialization.disabled=true",
        "-Dlogfile.name=" + STORM_SUPERVISOR_LOG_FILE,
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml"),
        ]
    exec_storm_class(
        "org.apache.storm.daemon.supervisor.Supervisor", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="supervisor",
        extrajars=cppaths,
        jvmopts=jvmopts,
        overriding_conf_file=args.config)


def ui(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = shlex.split(confvalue(
        "ui.childopts", storm_config_opts=storm_config_opts, extrapaths=cppaths, overriding_conf_file=args.config)
    ) + [
        "-Djava.deserialization.disabled=true",
        "-Dlogfile.name=ui.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml")
    ]

    allextrajars = get_wildcard_dir(STORM_WEBAPP_LIB_DIR)
    allextrajars.append(CLUSTER_CONF_DIR)
    exec_storm_class(
        "org.apache.storm.daemon.ui.UIServer", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="ui",
        jvmopts=jvmopts,
        extrajars=allextrajars,
        overriding_conf_file=args.config)


def logviewer(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = shlex.split(
        confvalue(
            "logviewer.childopts", storm_config_opts=storm_config_opts,
            extrapaths=cppaths, overriding_conf_file=args.config
        )
    ) + [
        "-Djava.deserialization.disabled=true",
        "-Dlogfile.name=logviewer.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml")
    ]

    allextrajars = get_wildcard_dir(STORM_WEBAPP_LIB_DIR)
    allextrajars.append(CLUSTER_CONF_DIR)
    exec_storm_class(
        "org.apache.storm.daemon.logviewer.LogviewerServer", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="logviewer",
        jvmopts=jvmopts,
        extrajars=allextrajars,
        overriding_conf_file=args.config)


def drpc_client(args):
    if not args.function and (len(args.function_arguments) % 2):
        raise argparse.ArgumentTypeError(
            "If no -f is supplied arguments need to be in the form [function arg]. " +
            "This has {} args".format(
                len(args.function_arguments)
            )
        )

    exec_storm_class(
        "org.apache.storm.command.BasicDrpcClient",
        main_class_args=remove_common_options(sys.argv[2:]), storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR],
        overriding_conf_file=args.config)


def drpc(args):
    cppaths = [CLUSTER_CONF_DIR]
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = shlex.split(
        confvalue(
            "drpc.childopts", storm_config_opts=storm_config_opts, extrapaths=cppaths, overriding_conf_file=args.config
        )
    ) + [
        "-Djava.deserialization.disabled=true",
        "-Dlogfile.name=drpc.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml")
    ]
    allextrajars = get_wildcard_dir(STORM_WEBAPP_LIB_DIR)
    allextrajars.append(CLUSTER_CONF_DIR)
    exec_storm_class(
        "org.apache.storm.daemon.drpc.DRPCServer", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="drpc",
        jvmopts=jvmopts,
        extrajars=allextrajars,
        overriding_conf_file=args.config)


def dev_zookeeper(args):
    storm_config_opts = get_config_opts(args.storm_config_opts)
    jvmopts = [
        "-Dlogfile.name=dev-zookeeper.log",
        "-Dlog4j.configurationFile=" + os.path.join(get_log4j2_conf_dir(storm_config_opts, args), "cluster.xml")
    ]
    exec_storm_class(
        "org.apache.storm.command.DevZookeeper", storm_config_opts=args.storm_config_opts,
        jvmtype="-server",
        daemonName="dev_zookeeper",
        jvmopts=jvmopts,
        extrajars=[CLUSTER_CONF_DIR],
        overriding_conf_file=args.config)


def version(args):
    exec_storm_class(
        "org.apache.storm.utils.VersionInfo", storm_config_opts=args.storm_config_opts,
        jvmtype="-client",
        extrajars=[CLUSTER_CONF_DIR],
        overriding_conf_file=args.config)


def print_classpath(args):
    print(get_classpath([], client=True))


def print_server_classpath(args):
    print(get_classpath([], daemon=True))


def monitor(args):
    exec_storm_class(
        "org.apache.storm.command.Monitor", storm_config_opts=args.storm_config_opts,
        main_class_args=remove_common_options(sys.argv[2:]),
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, STORM_BIN_DIR])

def main():
    init_storm_env()
    storm_parser = initialize_main_command()
    if len(sys.argv) == 1:
        storm_parser.print_help(sys.stderr)
        sys.exit(1)
    raw_args, unknown_args = storm_parser.parse_known_args()
    if hasattr(raw_args, "main_args"):
        raw_args.main_args += unknown_args
    raw_args.func(raw_args)


if __name__ == "__main__":
    main()
