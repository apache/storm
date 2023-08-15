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

import pprint
import traceback
from cmd import Cmd
import boto3


class AwsKafkaCmd(Cmd):
    """
    Refer to AWS document on Kafka client interface:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kafka.html
    """

    def __init__(self):
        super().__init__()
        self.client = boto3.client('kafka')
        self.cluster_info_list = None
        self.current_cluster_info = None

    def onecmd(self, line):
        """Override the command processor so it does not exit on exception in the command"""
        try:
            return super().onecmd(line)
        except Exception as ex:
            traceback.print_exc()
            return False  # don't stop

    def _print_pretty_dict(self, d, indent=4):
        """Print a dictionary in a pretty format"""
        if not d:
            return ""
        if isinstance(d, list):
            return "\n" + "\t" * indent + "\n    ".join([f'({i}) {self._print_pretty_dict(item)}' for i, item in enumerate(d)])
        elif isinstance(d, dict):
            return pprint.pformat(d, indent=indent)
        else:
            return str(d)

    def _confirm(self, prompt="Confirm "):
        confirm = input(prompt + "(yes/no)" )
        return confirm == "yes"

    def get_cluster_names(self):
        return [x['ClusterName'] for x in self.get_cluster_info_list()]

    def get_cluster_info_list(self):
        if not self.cluster_info_list:
            print("Finding current list of clusters")
            self.do_list(None)
        return self.cluster_info_list

    def set_current_cluster(self, cluster_name):
        cluster_names = self.get_cluster_names()
        for cluster_info in self.get_cluster_info_list():
            if cluster_name == cluster_info['ClusterName']:
                self.current_cluster_info = cluster_info
                return True
        else: # try partial match
            print(f'No cluster-name matched "{cluster_name}", trying partial match')
            for cluster_info in self.get_cluster_info_list():
                if cluster_name in cluster_info['ClusterName']:
                    print(f'Selecting cluster "{cluster_info["ClusterName"]}" as partial match to {cluster_name}')
                    self.current_cluster_info = cluster_info
                    return True
        return False

    def do_info(self, args):
        """List session information like clusters, selected cluster"""
        print(f'Clusters: {self.get_cluster_names()}')
        if self.current_cluster_info:
            print(f'Current cluster: {self.current_cluster_info["ClusterName"]}')
        else:
            print("Current cluster: None")

    def do_list(self, args):
        """Lists the clusters"""
        clusters = self.client.list_clusters()
        if clusters:
            self.cluster_info_list = clusters['ClusterInfoList']
            if self.cluster_info_list:
                for i, cluster_info in enumerate(self.cluster_info_list):
                    cluster_name = cluster_info["ClusterName"]
                    cluster_arn  = cluster_info["ClusterArn"]
                    res = self._print_pretty_dict(cluster_info, indent=4)
                    print(f'({i})\t{cluster_name}\t{cluster_arn}\n\n\t{res}\n')
            else:
                print('No Kafka clusters found')
        else:
            print('No response from AWS')

    def run_client_method_and_print_tag(self, cluster_name, client_method_name, result_tag, result_label=None, keyword_args=None):
        """
        Run a client method for the supplied cluster name, or current cluster name or all clusters.
        If cluster_name is supplied (in the args) then use it. Otherwise if the current cluster is set,
        return information on it. Otherwise, return the same information on all the clusters.

        :param cluster_name: arguments to the command, typically the name of the cluster
        :param client_method_name: method on the client to call
        :param result_tag: tag in the response that we are interested in
        :param result_label: Descriptive label for the result
        :param keyword_args:
        :return:
        """
        client_func = getattr(self.client, client_method_name)
        keyword_args = dict(keyword_args) if keyword_args else {}
        use_args = False if keyword_args else True   # do not use args if keyword_args is supplied
        if not result_label:
            result_label = result_tag
        if cluster_name:
            for cluster_info in self.get_cluster_info_list():
                if cluster_name in cluster_info['ClusterName']:
                    keyword_args['ClusterArn'] = cluster_info['ClusterArn']
                    result = client_func(**keyword_args)
                    info = self._print_pretty_dict(result[result_tag] if (result and result_tag) else result)
                    print(f'Cluster {cluster_info["ClusterName"]} {result_label}={info}')
        else:
            if self.current_cluster_info:
                keyword_args['ClusterArn'] = self.current_cluster_info['ClusterArn']
                result = client_func(**keyword_args)
                info = self._print_pretty_dict(result[result_tag] if (result and result_tag) else result)
                print(f'Cluster {self.current_cluster_info["ClusterName"]} {result_label}={info}')
            else:
                for cluster_info in self.get_cluster_info_list():
                    keyword_args['ClusterArn'] = cluster_info['ClusterArn']
                    result = client_func(**keyword_args)
                    info = self._print_pretty_dict(result[result_tag] if (result and result_tag) else result)
                    print(f'Cluster {cluster_info["ClusterName"]} {result_label}={info}')

    def do_bootstrap_brokers(self, args):
        """List Bootstrap brokers for the supplied cluster_name or or current cluster name or all clusters"""
        self.run_client_method_and_print_tag(args, 'get_bootstrap_brokers', 'BootstrapBrokerStringSaslIam',
                                             result_label='brokers')

    def do_describe(self, args):
        """Describe the supplied cluster_name or or current cluster name or all clusters"""
        self.run_client_method_and_print_tag(args, 'describe_cluster', 'ClusterInfo',
                                             result_label='brokers')

    def do_describev2(self, args):
        """Describe the supplied cluster_name or or current cluster name or all clusters"""
        self.run_client_method_and_print_tag(args, 'describe_cluster_v2', 'ClusterInfo',
                                             result_label='brokers')

    def do_compatible_kafka_versions(self, args):
        """Gets the Apache Kafka versions to which you can update the MSK cluster"""
        self.run_client_method_and_print_tag(args, 'get_compatible_kafka_versions', 'CompatibleKafkaVersions')

    def do_list_cluster_operations(self, args):
        """Returns a list of all the operations that have been performed on the specified MSK cluster."""
        self.run_client_method_and_print_tag(args, 'list_cluster_operations', 'ClusterOperationInfoList')

    def do_list_configurations(self, args):
        """Returns a list of all the MSK configurations in this Region"""
        self.run_client_method_and_print_tag(args, 'list_configurations', 'Configurations')

    def do_list_kafka_versions(self, args):
        """Returns a list of Apache Kafka versions"""
        self.run_client_method_and_print_tag(args, 'list_kafka_versions', 'KafkaVersions')

    def do_list_nodes(self, args):
        """Returns a list of the broker nodes in the cluster"""
        self.run_client_method_and_print_tag(args, 'list_nodes', 'NodeInfoList')

    def do_list_scram_secrets(self, args):
        """Returns a list of the Scram Secrets associated with an Amazon MSK cluster"""
        self.run_client_method_and_print_tag(args, 'list_scram_secrets', 'SecretArnList')

    def do_reboot_broker(self, args):
        """Reboots brokers"""
        if not self.current_cluster_info:
            print('Use select_cluster command first')
            return
        if self._confirm(prompt=f"Confirm reboot of broker {args}"):
            keyword_args = {
                'BrokerIds': [args],
                'ClusterArn': self.current_cluster_info['ClusterArn']}
            self.run_client_method_and_print_tag(None, 'reboot_broker', result_tag=None, keyword_args=keyword_args)

    def do_update_broker_count(self, args):
        """Updates the number of broker nodes in the cluster"""
        if not self.current_cluster_info:
            print('Use select_cluster command first')
            return
        if self._confirm(prompt=f"Confirm updating broker count to {args}"):
            keyword_args = {
                'ClusterArn': self.current_cluster_info['ClusterArn'],
                'CurrentVersion': self.current_cluster_info['CurrentVersion'],
                'TargetNumberOfBrokerNodes': int(args)
            }
            self.run_client_method_and_print_tag(None, 'update_broker_count', result_tag=None, keyword_args=keyword_args)

    def do_topics(self, args):
        """Lists the topics in the current cluster"""
        print("Not implemented yet - need to go thru MSK Client EC2 instance")

    def do_select_cluster(self, args):
        """Set the current cluster to the specified name if valid"""
        cluster_names = self.get_cluster_names()
        if len(self.get_cluster_info_list()) == 0:
            print("No clusters exist")
            return
        if args:
            if self.set_current_cluster(args):
                return
            print(f'Cluster "{args}" not not exist')
        print(f'syntax: select_cluster {cluster_names}')

    def do_quit(self, args):
        """Quits the program."""
        print("Quitting.")
        raise SystemExit


if __name__ == '__main__':
    prompt = AwsKafkaCmd()
    prompt.prompt = '> '
    prompt.cmdloop('Starting prompt...')
