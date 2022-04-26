#!/usr/bin/env python3

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


import unittest
import storm
import os


class Test(unittest.TestCase):
    """
    Mostly just test coverage
    """
    _testMethodName = None
    _testMethodDoc = None

    def __init__(self, method_name="None"):
        super().__init__()
        self._testMethodName = method_name
        storm.init_storm_env(within_unittest=True)

    def test_get_jars_full(self):
        storm.get_jars_full(".")

    def test_get_wildcard_dir(self):
        s = storm.get_wildcard_dir("./")
        self.assertEqual(s, ["./*"])

    def test_get_java_cmd(self):
        s = storm.get_java_cmd()
        expected = 'java' if not storm.is_windows() else 'java.exe'
        if storm.JAVA_HOME:
            expected = os.path.join(storm.JAVA_HOME, 'bin', expected)
        self.assertEqual(s, expected)

    def test_confvalue(self):
        name = 'name'
        storm_config_opts = {'ui.port': '8080'}
        extrapaths = []
        overriding_conf_file = None
        daemon = True
        s = storm.confvalue(name, storm_config_opts, extrapaths, overriding_conf_file, daemon)
        expected = ""
        self.assertEqual(s, expected)

    def test_get_classpath(self):
        extrajars = [f"jar{x}.jar" for x in range(5)]
        daemon = True
        client = False
        s = storm.get_classpath(extrajars, daemon, client)
        expected = ":".join(extrajars)
        self.assertEqual(s[-len(expected):], expected)

    def test_resolve_dependencies(self):
        artifacts = "org.apache.commons.commons-api"
        artifact_repositories = "maven-central"
        maven_local_repos_dir = "~/.m2"
        proxy_url = None
        proxy_username = None
        proxy_password = None
        try:
            output = storm.resolve_dependencies(artifacts, artifact_repositories, maven_local_repos_dir,
                                                proxy_url, proxy_username, proxy_password)
        except RuntimeError as ex:
            print(f"Unexpected {ex=}, {type(ex)=}")
        # test coverage only

    def test_exec_storm_class(self):
        klass = "org.apache.storm.starter.WordCountTopology"
        storm_config_opts = []
        jvmtype = "-server"
        jvmopts = []
        extrajars = []
        main_class_args = []
        fork = False
        daemon = True
        client = False
        daemonName = ""
        overriding_conf_file = None
        # exit_code = storm.exec_storm_class(klass, storm_config_opts=storm_config_opts, jvmtype=jvmtype, jvmopts=jvmopts,
        #                                    extrajars=extrajars, main_class_args=main_class_args, fork=fork,
        #                                    daemon=daemon, client=client, daemonName=daemonName,
        #                                    overriding_conf_file=overriding_conf_file)

    def test_run_client_jar(self):
        pass

    def test_print_localconfvalue(self):
        class Args:
            conf_name = self._testMethodName
            storm_config_opts = {self._testMethodName: "confvalue"}
            config = "config/file/path.yaml"

        args = Args()
        storm.print_localconfvalue(args)

    def test_print_remoteconfvalue(self):
        class Args:
            conf_name = self._testMethodName
            storm_config_opts = {self._testMethodName: "confvalue"}
            config = "config/file/path.yaml"

        args = Args()
        storm.print_remoteconfvalue(args)

    def test_initialize_main_command(self):
        storm.initialize_main_command()

    def test_jar(self):
        pass

    def test_local(self):
        pass

    def test_sql(self):
        pass

    def test_kill(self):
        pass

    def test_upload_credentials(self):
        pass

    def test_blob(self):
        pass

    def test_heartbeats(self):
        pass

    def test_activate(self):
        pass

    def test_listtopos(self):
        pass

    def test_set_log_level(self):
        pass

    def test_deactivate(self):
        pass

    def test_rebalance(self):
        pass

    def test_get_errors(self):
        pass

    def test_healthcheck(self):
        pass

    def test_kill_workers(self):
        pass

    def test_admin(self):
        pass

    def test_shell(self):
        pass

    def test_repl(self):
        pass

    def test_get_log4j2_conf_dir(self):
        pass

    def test_nimbus(self):
        pass

    def test_pacemaker(self):
        pass

    def test_supervisor(self):
        pass

    def test_ui(self):
        pass

    def test_logviewer(self):
        pass

    def test_drpc_client(self):
        pass

    def test_drpc(self):
        pass

    def test_dev_zookeeper(self):
        pass

    def test_version(self):
        pass

    def test_print_classpath(self):
        storm.print_classpath(None)

    def test_print_server_classpath(self):
        storm.print_server_classpath(None)

    def test_monitor(self):
        pass


