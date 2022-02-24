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

from unittest import TestCase, main as test_main
import mock
import sys
import os


class TestStormCli(TestCase):

    def setUp(self):
        self.mock_path_exists = mock.patch("os.path.exists").start()
        self.mock_popen = mock.patch("subprocess.Popen").start()
        self.mock_execvp = mock.patch("os.execvp").start()
        self.mock_path_exists.return_value = True
        self.mock_popen.return_value.returncode = 0
        self.mock_popen.return_value.communicate = \
            mock.MagicMock(return_value=("{}", ""))
        self.storm_dir = os.path.abspath(
            os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir)
        )
        from storm import main as cli_main
        self.cli_main = cli_main
        self.java_cmd = os.path.join(
            os.getenv('JAVA_HOME', None), 'bin', 'java'
        )

    def base_test(self, command_invocation, mock_shell_interface, expected_output):
        print(command_invocation)
        with mock.patch.object(sys, "argv", command_invocation):
            self.cli_main()
        if expected_output not in mock_shell_interface.call_args_list:
            print("Expected:" + str(expected_output))
            print("Got:" + str(mock_shell_interface.call_args_list[-1]))
        assert expected_output in mock_shell_interface.call_args_list

    def test_jar_command(self):
        self.base_test([
            'storm', 'jar',
            'example/storm-starter/storm-starter-topologies-*.jar',
            'org.apache.storm.starter.RollingTopWords', 'blobstore-remote2',
            'remote', '-c topology.blobstore.map=\'{"key1":{"localname":"blob_file", "uncompress":false},"key2":{}}\'', '--jars',
            './external/storm-redis/storm-redis-1.1.0.jar,./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"', '--artifacts', '"redis.clients:jedis:2.9.0,org.apache.kafka:kafka-clients:1.0.0^org.slf4j:slf4j-api"', '--artifactRepositories', '"jboss-repository^http://repository.jboss.com/maven2,HDPRepo^http://repo.hortonworks.com/content/groups/public/'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=+topology.blobstore.map%3D%27%7B%22key1%22%3A%7B%22localname%22%3A%22blob_file%22%2C+%22uncompress%22%3Afalse%7D%2C%22key2%22%3A%7B%7D%7D%27',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib-worker:' + self.storm_dir
                + '/extlib:example/storm-starter/storm-starter-topologies-*.jar:' + self.storm_dir + '/conf:'
                + self.storm_dir + '/bin:./external/storm-redis/storm-redis-1.1.0.jar:./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"', '-Dstorm.jar=example/storm-starter/storm-starter-topologies-*.jar', '-Dstorm.dependency.jars=./external/storm-redis/storm-redis-1.1.0.jar,./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"', '-Dstorm.dependency.artifacts={}',
                'org.apache.storm.starter.RollingTopWords', 'blobstore-remote2', 'remote'
            ])
        )

        self.mock_execvp.reset_mock()

        self.base_test([
            'storm', 'jar', '/path/to/jar.jar', 'some.Topology.Class',
            '-name', 'run-topology', 'randomArgument', '-randomFlag', 'randomFlagValue', '-rotateSize', '0.0001',
            '--hdfsConf', 'someOtherHdfsConf', 'dfs.namenode.kerberos.principal.pattern=hdfs/*.EV..COM'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib-worker:' + self.storm_dir
                + '/extlib:/path/to/jar.jar:' + self.storm_dir + '/conf:' + self.storm_dir + '/bin:',
                '-Dstorm.jar=/path/to/jar.jar', '-Dstorm.dependency.jars=', '-Dstorm.dependency.artifacts={}',
                'some.Topology.Class', '-name', 'run-topology', 'randomArgument', '-randomFlag', 'randomFlagValue',
                '-rotateSize', '0.0001', '--hdfsConf', 'someOtherHdfsConf',
                'dfs.namenode.kerberos.principal.pattern=hdfs/*.EV..COM'
            ])
        )

    def test_localconfvalue_command(self):
        self.base_test(
            ["storm", "localconfvalue", "conf_name"], self.mock_popen, mock.call([
             self.java_cmd, '-client', '-Dstorm.options=',
             '-Dstorm.conf.file=', '-cp',  self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +'/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf',
             'org.apache.storm.command.ConfigValue', 'conf_name'
             ], stdout=-1
            )
        )

    def test_remoteconfvalue_command(self):
        self.base_test(
            ["storm", "remoteconfvalue", "conf_name"], self.mock_popen, mock.call([
             self.java_cmd, '-client', '-Dstorm.options=',
             '-Dstorm.conf.file=', '-cp',  self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf',
             'org.apache.storm.command.ConfigValue', 'conf_name'
             ], stdout=-1
            )
        )

    def test_local_command(self):
        self.base_test([
            'storm', 'local',
            'example/storm-starter/storm-starter-topologies-*.jar',
            'org.apache.storm.starter.RollingTopWords', 'blobstore-remote2',
            'remote', '--jars',
            './external/storm-redis/storm-redis-1.1.0.jar,./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"',
            '--artifacts', '"redis.clients:jedis:2.9.0,org.apache.kafka:kafka-clients:1.0.0^org.slf4j:slf4j-api"',
            '--artifactRepositories',
            '"jboss-repository^http://repository.jboss.com/maven2,HDPRepo^http://repo.hortonworks.com/content/groups/public/'
            ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client','-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:example/storm-starter/storm-starter-topologies-*.jar:' + self.storm_dir +
                '/conf:' + self.storm_dir +
                '/bin:./external/storm-redis/storm-redis-1.1.0.jar:./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"',
                '-Dstorm.local.sleeptime=20', '-Dstorm.jar=example/storm-starter/storm-starter-topologies-*.jar',
                '-Dstorm.dependency.jars=./external/storm-redis/storm-redis-1.1.0.jar,./external/storm-kafka-client/storm-kafka-client-1.1.0.jar"',
                '-Dstorm.dependency.artifacts={}', 'org.apache.storm.LocalCluster',
                'org.apache.storm.starter.RollingTopWords',
                'blobstore-remote2', 'remote'
            ])
        )

    def test_sql_command(self):
        self.base_test(
            ['storm', 'sql', 'apache_log_error_filtering.sql', 'apache_log_error_filtering', '--artifacts',
             '"org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka:2.0.0-SNAPSHOT,org.apache.kafka:kafka_2.10:0.8.2.2^org.slf4j:slf4j-log4j12,org.apache.kafka:kafka-clients:0.8.2.2"'
            ], self.mock_execvp, mock.call(
                self.java_cmd,
                [self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                 '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                 '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                 self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' +
                 self.storm_dir +
                 '/conf:' + self.storm_dir + '/bin:' + self.storm_dir + '/lib-tools/sql/core',\
                 '-Dstorm.dependency.jars=', '-Dstorm.dependency.artifacts={}',
                 'org.apache.storm.sql.StormSqlRunner', '--file', 'apache_log_error_filtering.sql',
                 '--topology', 'apache_log_error_filtering']
            )
        )

    def test_kill_command(self):
        self.base_test([
            'storm', 'kill', 'doomed_topology'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir + '/bin', 'org.apache.storm.command.KillTopology', 'doomed_topology'
            ])
        )

    def test_upload_credentials_command(self):
        self.base_test([
            'storm', 'upload-credentials', '--config', '/some/other/storm.yaml', '-c', 'test=test', 'my-topology-name', 'appids', 'role.name1,role.name2'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd,  '-client', '-Ddaemon.name=', '-Dstorm.options=test%3Dtest',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=/some/other/storm.yaml',
                '-cp', self.storm_dir + '/*:' + self.storm_dir + '/lib:' +
                                             self.storm_dir +
                                             '/extlib:' + self.storm_dir + '/extlib-daemon:' +
                                             self.storm_dir + '/conf:' + self.storm_dir +
                                             '/bin', 'org.apache.storm.command.UploadCredentials',
                'my-topology-name', 'appids', 'role.name1,role.name2'])
        )

    def test_blobstore_command(self):
        self.base_test([
            'storm', 'blobstore', 'create', 'mytopo:data.tgz', '-f', 'data.tgz', '-a', 'u:alice:rwa,u:bob:rw,o::r'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.Blobstore', 'create',
                'mytopo:data.tgz', '-f', 'data.tgz', '-a', 'u:alice:rwa,u:bob:rw,o::r'])
        )
        self.mock_execvp.reset_mock()

        self.base_test([
            'storm', 'blobstore', 'list'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir,
                '-Dstorm.log.dir=' + self.storm_dir + "/logs", '-Djava.library.path=',
                '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.Blobstore', 'list'])
        )
        self.mock_execvp.reset_mock()

        self.base_test([
            'storm', 'blobstore', 'list', 'wordstotrack'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir,
                '-Dstorm.log.dir=' + self.storm_dir + "/logs", '-Djava.library.path=',
                '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.Blobstore', 'list', 'wordstotrack'])
        )
        self.mock_execvp.reset_mock()

        self.base_test([
            'storm', 'blobstore', 'update', '-f', '/wordsToTrack.list', 'wordstotrack'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.Blobstore', 'update', '-f',
                '/wordsToTrack.list', 'wordstotrack'])
        )
        self.mock_execvp.reset_mock()

        self.base_test([
            'storm', 'blobstore', 'cat', 'wordstotrack'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.Blobstore', 'cat', 'wordstotrack'])
        )

    def test_activate_command(self):
        self.base_test([
            'storm', 'activate', 'doomed_topology'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir + '/bin',
                'org.apache.storm.command.Activate', 'doomed_topology'
            ])
        )

    def test_deactivate_command(self):
        self.base_test([
            'storm', 'deactivate', 'doomed_topology'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir +
                '/bin', 'org.apache.storm.command.Deactivate', 'doomed_topology'
            ])
        )

    def test_rebalance_command(self):
        self.base_test([
            'storm', 'rebalance', 'doomed_topology'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir +
                '/bin', 'org.apache.storm.command.Rebalance', 'doomed_topology'
            ])
        )

    def test_list_command(self):
        self.base_test([
            'storm', 'list'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir +
                '/bin', 'org.apache.storm.command.ListTopologies'
            ])
        )

    def test_nimbus_command(self):
        self.base_test([
            'storm', 'nimbus'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=nimbus', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=nimbus.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.daemon.nimbus.Nimbus'
            ])
        )

    def test_supervisor_command(self):
        self.base_test([
            'storm', 'supervisor'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=supervisor', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=supervisor.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.daemon.supervisor.Supervisor'
            ])
        )

    def test_pacemaker_command(self):
        self.base_test([
            'storm', 'pacemaker'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=pacemaker', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=pacemaker.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.pacemaker.Pacemaker'
            ])
        )

    def test_ui_command(self):
        self.base_test([
            'storm', 'ui'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=ui', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir +
                '/lib-webapp:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=ui.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.daemon.ui.UIServer'
            ])
        )

    def test_logviewer_command(self):
        self.base_test([
            'storm', 'logviewer'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=logviewer', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir +
                '/lib-webapp:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=logviewer.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.daemon.logviewer.LogviewerServer'
            ])
        )

    def test_drpc_command(self):
        self.base_test([
            'storm', 'drpc'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-server', '-Ddaemon.name=drpc', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir +
                '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir +
                '/lib-webapp:' + self.storm_dir + '/conf',
                '-Djava.deserialization.disabled=true', '-Dlogfile.name=drpc.log',
                '-Dlog4j.configurationFile=' + self.storm_dir + '/log4j2/cluster.xml',
                'org.apache.storm.daemon.drpc.DRPCServer'
            ])
        )

    def test_drpc_client_command(self):
        self.base_test([
            'storm', 'drpc-client', 'exclaim', 'a', 'exclaim', 'b', 'test', 'bar'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir +
                '/bin', 'org.apache.storm.command.BasicDrpcClient', 'exclaim', 'a', 'exclaim', 'b', 'test', 'bar'
            ])
        )
        self.base_test([
            'storm', 'drpc-client', '-f', 'exclaim', 'a', 'b'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs",
                '-Djava.library.path=', '-Dstorm.conf.file=', '-cp',
                self.storm_dir + '/*:' + self.storm_dir + '/lib:' + self.storm_dir + '/extlib:' + self.storm_dir +
                '/extlib-daemon:' + self.storm_dir + '/conf:' + self.storm_dir +
                '/bin', 'org.apache.storm.command.BasicDrpcClient', '-f', 'exclaim', 'a', 'b'
            ])
        )

    def test_healthcheck_command(self):
        self.base_test([
            'storm', 'node-health-check'
        ], self.mock_execvp, mock.call(
            self.java_cmd, [
                self.java_cmd, '-client', '-Ddaemon.name=', '-Dstorm.options=',
                '-Dstorm.home=' + self.storm_dir, '-Dstorm.log.dir=' + self.storm_dir + "/logs", '-Djava.library.path=',
                '-Dstorm.conf.file=', '-cp', self.storm_dir + '/*:' + self.storm_dir + '/lib:' +
                self.storm_dir + '/extlib:' + self.storm_dir + '/extlib-daemon:' + self.storm_dir + '/conf:' +
                self.storm_dir + '/bin', 'org.apache.storm.command.HealthCheck'
            ])
        )



    def tearDown(self):
        self.mock_popen.stop()
        self.mock_path_exists.stop()


if __name__ == '__main__':
    test_main()