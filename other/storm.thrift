#!/usr/local/bin/thrift --gen java:beans,nocamel,hashcode

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

namespace java backtype.storm.generated

union JavaObjectArg {
  1: i32 int_arg;
  2: i64 long_arg;
  3: string string_arg;
  4: bool bool_arg;
  5: binary binary_arg;
  6: double double_arg;
}

struct JavaObject {
  1: required string full_class_name;
  2: required list<JavaObjectArg> args_list;
}

struct NullStruct {
  
}

struct GlobalStreamId {
  1: required string componentId;
  2: required string streamId;
  #Going to need to add an enum for the stream type (NORMAL or FAILURE)
}

union Grouping {
  1: list<string> fields; //empty list means global grouping
  2: NullStruct shuffle; // tuple is sent to random task
  3: NullStruct all; // tuple is sent to every task
  4: NullStruct none; // tuple is sent to a single task (storm's choice) -> allows storm to optimize the topology by bundling tasks into a single process
  5: NullStruct direct; // this bolt expects the source bolt to send tuples directly to it
  6: JavaObject custom_object;
  7: binary custom_serialized;
  8: NullStruct local_or_shuffle; // prefer sending to tasks in the same worker process, otherwise shuffle
  9: NullStruct localFirst; //  local worker shuffle > local node shuffle > other node shuffle 
}

struct StreamInfo {
  1: required list<string> output_fields;
  2: required bool direct;
}

struct ShellComponent {
  // should change this to 1: required list<string> execution_command;
  1: string execution_command;
  2: string script;
}

union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}

struct ComponentCommon {
  1: required map<GlobalStreamId, Grouping> inputs;
  2: required map<string, StreamInfo> streams; //key is stream id
  3: optional i32 parallelism_hint; //how many threads across the cluster should be dedicated to this component

  // component specific configuration respects:
  // topology.debug: false
  // topology.max.task.parallelism: null // can replace isDistributed with this
  // topology.max.spout.pending: null
  // topology.kryo.register // this is the only additive one
  
  // component specific configuration
  4: optional string json_conf;
}

struct SpoutSpec {
  1: required ComponentObject spout_object;
  2: required ComponentCommon common;
  // can force a spout to be non-distributed by overriding the component configuration
  // and setting TOPOLOGY_MAX_TASK_PARALLELISM to 1
}

struct Bolt {
  1: required ComponentObject bolt_object;
  2: required ComponentCommon common;
}

// not implemented yet
// this will eventually be the basis for subscription implementation in storm
struct StateSpoutSpec {
  1: required ComponentObject state_spout_object;
  2: required ComponentCommon common;
}

struct StormTopology {
  //ids must be unique across maps
  // #workers to use is in conf
  1: required map<string, SpoutSpec> spouts;
  2: required map<string, Bolt> bolts;
  3: required map<string, StateSpoutSpec> state_spouts;
}

exception AlreadyAliveException {
  1: required string msg;
}

exception NotAliveException {
  1: required string msg;
}

exception AuthorizationException {
  1: required string msg;
}

exception InvalidTopologyException {
  1: required string msg;
}

exception TopologyAssignException {
  1: required string msg;
}

struct TopologySummary {
  1: required string id;
  2: required string name;
  3: required string status;
  4: required i32 uptimeSecs;
  5: required i32 numTasks;
  6: required i32 numWorkers;
  7: optional string errorInfo;
}

struct SupervisorSummary {
  1: required string host;
  2: required string supervisorId;
  3: required i32 uptimeSecs;
  4: required i32 numWorkers;
  5: required i32 numUsedWorkers;
}

struct NimbusStat {
  1: required string host;
  2: required string uptimeSecs;
}

struct NimbusSummary {
  1: required NimbusStat nimbusMaster;
  2: required list<NimbusStat> nimbusSlaves;
  3: required i32 supervisorNum;
  4: required i32 totalPortNum;
  5: required i32 usedPortNum;
  6: required i32 freePortNum;
  7: required string version;
}

struct ClusterSummary {
  1: required NimbusSummary nimbus;
  2: required list<SupervisorSummary> supervisors;
  3: required list<TopologySummary> topologies;
}

struct TaskComponent {
  1: required i32 taskId
  2: required string component;
}

struct WorkerSummary {
  1: required i32 port;
  2: required i32 uptime;
  3: required string topology;
  4: required list<TaskComponent> tasks
}

struct MetricWindow {
  // map<second, double>, 0 means all time
  1: required map<i32, double> metricWindow;
}

// a union type for counter, gauge, meter, histogram and timer
struct MetricSnapshot {
  1: required i64 metricId;
  2: required i64 ts;
  3: required i32 metricType;
  4: optional i64 longValue;
  5: optional double doubleValue;
  6: optional double m1;
  7: optional double m5;
  8: optional double m15;
  9: optional double mean;
  10: optional i64 min;
  11: optional i64 max;
  12: optional double p50;
  13: optional double p75;
  14: optional double p95;
  15: optional double p98;
  16: optional double p99;
  17: optional double p999;
  18: optional double stddev;
  19: optional list<i64> points;
}

struct MetricInfo {
  // map<metricName, map<window, snapshot>>
  1: optional map<string, map<i32, MetricSnapshot>> metrics;
}

struct SupervisorWorkers {
  1: required SupervisorSummary supervisor;
  2: required list<WorkerSummary> workers;
  3: required map<string,MetricInfo> workerMetric;
}

struct ErrorInfo {
  1: required string error;
  2: required i32 errorTimeSecs;
}

struct ComponentSummary {
  1: required string name;
  2: required i32 parallel;
  3: required string type;
  4: required list<i32> taskIds;
  5: optional list<ErrorInfo>  errors;
}

struct TaskSummary {
  1: required i32 taskId;
  2: required i32 uptime;
  3: required string status;
  4: required string host;
  5: required i32 port;
  6: optional list<ErrorInfo>  errors;
}

struct TopologyMetric {
  1: required MetricInfo topologyMetric;
  2: required MetricInfo componentMetric;
  3: required MetricInfo workerMetric;
  4: required MetricInfo taskMetric;
  5: required MetricInfo streamMetric;
  6: required MetricInfo nettyMetric;
}

struct TopologyInfo {
  1: required TopologySummary topology;
  2: required list<ComponentSummary> components;
  3: required list<TaskSummary> tasks;
  4: required TopologyMetric metrics;
}

struct WorkerUploadMetrics {
  1: required string topologyId;
  2: required string supervisorId;
  3: required i32 port;
  4: required MetricInfo allMetrics;
}

struct KillOptions {
  1: optional i32 wait_secs;
}

struct RebalanceOptions {
  1: optional i32 wait_secs;
  2: optional bool reassign;
  3: optional string conf;
}

struct Credentials {
  1: required map<string,string> creds;
}

enum TopologyInitialStatus {
    ACTIVE = 1,
    INACTIVE = 2
}
struct SubmitOptions {
  1: required TopologyInitialStatus initial_status;
  2: optional Credentials creds;
}

struct MonitorOptions {
  1: optional bool isEnable;
}



struct ThriftSerializedObject {
  1: required string name;
  2: required binary bits;
}

struct LocalStateData {
   1: required map<string, ThriftSerializedObject> serialized_parts;
}

struct TaskHeartbeat {
    1: required i32 time;
    2: required i32 uptime;
}

struct TopologyTaskHbInfo {
    1: required string topologyId;
    2: required i32 topologyMasterId;
    3: optional map<i32, TaskHeartbeat> taskHbs;
}

service Nimbus {
  void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3: TopologyAssignException tae);
  void submitTopologyWithOpts(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology, 5: SubmitOptions options) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3:TopologyAssignException tae);
  void killTopology(1: string name) throws (1: NotAliveException e);
  void killTopologyWithOpts(1: string name, 2: KillOptions options) throws (1: NotAliveException e);
  void activate(1: string name) throws (1: NotAliveException e);
  void deactivate(1: string name) throws (1: NotAliveException e);
  void rebalance(1: string name, 2: RebalanceOptions options) throws (1: NotAliveException e, 2: InvalidTopologyException ite);
  void metricMonitor(1: string name, 2: MonitorOptions options) throws (1: NotAliveException e);
  void restart(1: string name, 2: string jsonConf) throws (1: NotAliveException e, 2: InvalidTopologyException ite, 3: TopologyAssignException tae);

  // need to add functions for asking about status of storms, what nodes they're running on, looking at task logs

  void beginLibUpload(1: string libName);
  string beginFileUpload();
  void uploadChunk(1: string location, 2: binary chunk);
  void finishFileUpload(1: string location);

  string beginFileDownload(1: string file);
  //can stop downloading chunks when receive 0-length byte array back
  binary downloadChunk(1: string id);
  void finishFileDownload(1: string id);

  // returns json
  string getNimbusConf();
  //returns json
  string getTopologyConf(1: string id) throws (1: NotAliveException e);
  string getTopologyId(1: string topologyName) throws (1: NotAliveException e);

  // stats functions
  ClusterSummary getClusterInfo();
  SupervisorWorkers getSupervisorWorkers(1: string host) throws (1: NotAliveException e);
  TopologyInfo getTopologyInfo(1: string id) throws (1: NotAliveException e);
  TopologyInfo getTopologyInfoByName(1: string topologyName) throws (1: NotAliveException e);

  StormTopology getTopology(1: string id) throws (1: NotAliveException e);
  StormTopology getUserTopology(1: string id) throws (1: NotAliveException e);

  // relate metric
  void uploadTopologyMetrics(1: string topologyId, 2: TopologyMetric topologyMetrics);
  map<string,i64> registerMetrics(1: string topologyId, 2: set<string> metrics);
  TopologyMetric getTopologyMetrics(1: string topologyId);

  // get metrics by type: component/task/stream/worker/topology
  list<MetricInfo> getMetrics(1: string topologyId, 2: i32 type);

  MetricInfo getNettyMetrics(1:string topologyId);
  MetricInfo getNettyMetricsByHost(1: string topologyId, 2: string host);
  MetricInfo getPagingNettyMetrics(1: string topologyId, 2: string host, 3: i32 page);
  i32 getNettyMetricSizeByHost(1: string topologyId, 2: string host);

  MetricInfo getTaskMetrics(1:string topologyId, 2:string component);
  list<MetricInfo> getTaskAndStreamMetrics(1:string topologyId, 2: i32 taskId);

  // get only topology level metrics
  list<MetricInfo> getSummarizedTopologyMetrics(1: string topologyId);

  string getVersion();

  void updateTopology(1: string name, 2: string uploadedLocation, 3: string updateConf) throws (1: NotAliveException e, 2: InvalidTopologyException ite);

  void updateTaskHeartbeat(1: TopologyTaskHbInfo taskHbs);
}

struct DRPCRequest {
  1: required string func_args;
  2: required string request_id;
}

exception DRPCExecutionException {
  1: required string msg;
}

service DistributedRPC {
  string execute(1: string functionName, 2: string funcArgs) throws (1: DRPCExecutionException e, 2: AuthorizationException aze);
}

service DistributedRPCInvocations {
  void result(1: string id, 2: string result) throws (1: AuthorizationException aze);
  DRPCRequest fetchRequest(1: string functionName) throws (1: AuthorizationException aze);
  void failRequest(1: string id) throws (1: AuthorizationException aze);  
}
