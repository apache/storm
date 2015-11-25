/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.ui.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.jstorm.ui.model.graph.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.ModelMap;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.model.ClusterConfig;
import com.alibaba.jstorm.ui.model.ClusterEntity;
import com.alibaba.jstorm.ui.model.Response;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.NimbusSummary;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIUtils {

    private static final Logger LOG = LoggerFactory.getLogger(UIUtils.class);

    public static Map<String, ClusterConfig> clusterConfig = new HashMap<>();
    public static Map<String, ClusterEntity> clustersCache = new HashMap<>();
    public static Map uiConfig = null;
    public static boolean isInitialized = false;

    private static long uiConfigLastModified = 0;
    private static String confPath = System.getProperty("user.home") + File.separator + ".jstorm" + File.separator
            + "storm.yaml";
    private static DateFormat dayFormat = new SimpleDateFormat("MM-dd HH:mm");
    private static DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm");


    private static ScheduledExecutorService poolThread = Executors.newScheduledThreadPool(1);

    static {
        readUiConfig();
        //refresh the cluster cache every 3 minutes
        poolThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flushClusterCache();
                LOG.info("refresh the cluster cache, total active cluster:{}", NimbusClientManager.getClientSize());
            }
        }, 3, 3, TimeUnit.MINUTES);
    }

    public static Map readUiConfig() {
        File file = new File(confPath);

        //check whether ui config is update, if not , skip reload config
        if (!isFileModified(file)) {
            return uiConfig;
        }

        // reload config
        Map ret = Utils.readStormConfig();
        ret.remove(Config.STORM_ZOOKEEPER_ROOT);
        ret.remove(Config.STORM_ZOOKEEPER_SERVERS);
        ret.remove("cluster.name");
        if (file.exists()) {
            FileInputStream fileStream;
            try {
                fileStream = new FileInputStream(file);
                Yaml yaml = new Yaml();

                Map clientConf = (Map) yaml.load(fileStream);

                if (clientConf != null) {
                    ret.putAll(clientConf);
                }
            } catch (FileNotFoundException e) {
            }
            if (!ret.containsKey(Config.NIMBUS_HOST)) {
                ret.put(Config.NIMBUS_HOST, "localhost");

            }
            uiConfig = ret;
            isInitialized = false;
            //flush cluster config
            flushClusterConfig();
            //flush cluster cache
            flushClusterCache();
        }
        return uiConfig;
    }

    private static boolean isFileModified(File file) {
        long lastModified = file.lastModified();
        if (uiConfigLastModified == 0 || uiConfigLastModified < lastModified) {
            uiConfigLastModified = lastModified;
            return true;
        } else {
            return false;
        }
    }

    private static void flushClusterCache() {
        NimbusClient client = null;
        Map<String, ClusterEntity> clusterEntities = new HashMap<>();
        for (String name : UIUtils.clusterConfig.keySet()) {
            try {
                client = NimbusClientManager.getNimbusClient(name);
                int port = getNimbusPort(name);
                clusterEntities.put(name, getClusterEntity(client.getClient().getClusterInfo(), name, port));
            } catch (Exception e) {
                NimbusClientManager.removeClient(name);
                LOG.error(e.getMessage(), e);
            }
        }
        clustersCache = clusterEntities;
    }

    public static ClusterEntity getClusterEntity(ClusterSummary cluster, String cluster_name, int port) {
        int topology_num = cluster.get_topologies_size();
        NimbusSummary ns = cluster.get_nimbus();
        return getClusterEntity(ns, topology_num, cluster_name, port);
    }

    public static ClusterEntity getClusterEntity(NimbusSummary ns, int topology_num, String cluster_name, int port) {
        String ip = ns.get_nimbusMaster().get_host().split(":")[0];
        ClusterEntity ce = new ClusterEntity(cluster_name, ns.get_supervisorNum(),
                topology_num, ip, port, ns.get_version(), ns.get_totalPortNum(), ns.get_usedPortNum());
        return ce;
    }


    public static void flushClusterConfig() {
        Map<String, ClusterConfig> configMap = new HashMap<>();

        //ui.cluster
        List<Map> uiClusters = ConfigExtension.getUiClusters(uiConfig);
        if (uiClusters != null && uiClusters.size() > 0) {
            for (Map cluster : uiClusters) {
                String zkRoot = ConfigExtension.getUiClusterZkRoot(cluster);
                String clusterName = ConfigExtension.getUiClusterName(cluster);
                if (clusterName == null) clusterName = parseClusterName(zkRoot);
                List<String> zkServers = ConfigExtension.getUiClusterZkServers(cluster);
                Integer zkPort = ConfigExtension.getUiClusterZkPort(cluster);
                ClusterConfig conf = new ClusterConfig(zkRoot, zkServers, zkPort);
                if (isInitialized) {
                    Map<String, Object> nimbusConf = getNimbusConf(clusterName);
                    Integer nimbusPort = ConfigExtension.getNimbusDeamonHttpserverPort(nimbusConf);
                    Integer supervisorPort = ConfigExtension.getSupervisorDeamonHttpserverPort(nimbusConf);
                    conf.setNimbusPort(nimbusPort);
                    conf.setSupervisorPort(supervisorPort);
                }
                if (conf.isAvailable()) {
                    configMap.put(clusterName, conf);
                }
            }
        } else {
            String zkRoot = (String) uiConfig.get(Config.STORM_ZOOKEEPER_ROOT);
            String clusterName = ConfigExtension.getClusterName(uiConfig);
            if (clusterName == null) clusterName = parseClusterName(zkRoot);
            List<String> zkServers = (List<String>) uiConfig.get(Config.STORM_ZOOKEEPER_SERVERS);
            Integer zkPort = JStormUtils.parseInt(uiConfig.get(Config.STORM_ZOOKEEPER_PORT));
            ClusterConfig conf = new ClusterConfig(zkRoot, zkServers, zkPort);

            if (isInitialized) {
                Map<String, Object> nimbusConf = getNimbusConf(clusterName);
                conf.setNimbusPort(ConfigExtension.getNimbusDeamonHttpserverPort(nimbusConf));
                conf.setSupervisorPort(ConfigExtension.getSupervisorDeamonHttpserverPort(nimbusConf));
            }
            if (conf.isAvailable()) {
                configMap.put(clusterName, conf);
            }
        }
        clusterConfig = configMap;
        isInitialized = true;
        LOG.debug("nimbus config: " + clusterConfig);
    }

    private static String parseClusterName(String zkRoot) {
        if (zkRoot == null) {
            return UIDef.DEFAULT_CLUSTER_NAME;
        } else if (zkRoot.startsWith("/")) {
            return zkRoot.substring(1);
        } else {
            return zkRoot;
        }
    }

    public static String getHostIp(String ipPort) {

        if (ipPort.contains(":")) {
            String firstPart = ipPort.substring(0, ipPort.indexOf(":"));
            String lastPart = ipPort.substring(ipPort.indexOf(":"));

            return NetWorkUtils.ip2Host(firstPart) + lastPart;
        } else {
            return NetWorkUtils.ip2Host(ipPort);
        }

    }

    public static String getHost(String ipPort) {
        if (ipPort.contains(":")) {
            String firstPart = ipPort.substring(0, ipPort.indexOf(":"));

            return NetWorkUtils.ip2Host(firstPart);
        } else {
            return NetWorkUtils.ip2Host(ipPort);
        }
    }

    public static String getIp(String hostPort) {
        if (hostPort.contains(":")) {
            String firstPart = hostPort.substring(0, hostPort.indexOf(":"));

            return NetWorkUtils.host2Ip(firstPart);
        } else {
            return NetWorkUtils.host2Ip(hostPort);
        }
    }

    public static String getPort(String hostPort) {
        if (hostPort.contains(":")) {
            String lastPart = hostPort.substring(hostPort.indexOf(":"));

            return lastPart;
        } else {
            return null;
        }
    }

    public static Integer parseWindow(String windowStr) {
        Integer window;
        if (StringUtils.isBlank(windowStr)) {
            window = AsmWindow.M1_WINDOW;
        } else {
            window = Integer.valueOf(windowStr);
        }
        return window;
    }


    //to get nimbus client, we should reset ZK config
    public static Map resetZKConfig(Map conf, String clusterName) {
        ClusterConfig nimbus = clusterConfig.get(clusterName);
        if (nimbus == null) return conf;
        conf.put(Config.STORM_ZOOKEEPER_ROOT, nimbus.getZkRoot());
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, nimbus.getZkServers());
        conf.put(Config.STORM_ZOOKEEPER_PORT, nimbus.getZkPort());
        return conf;
    }

    public static Integer getNimbusPort(String name) {
        if (clusterConfig.containsKey(name)) {
            Integer port = clusterConfig.get(name).getNimbusPort();
            if (port == null) {
                flushClusterConfig();
                port = clusterConfig.get(name).getNimbusPort();
            }
            return port;
        }
        LOG.info("can not find the cluster with name:" + name);
        return null;
    }

    public static Integer getSupervisorPort(String name) {
        if (clusterConfig.containsKey(name)) {
            Integer port = clusterConfig.get(name).getSupervisorPort();
            if (port == null) {
                flushClusterConfig();
                port = clusterConfig.get(name).getSupervisorPort();
            }
            return port;
        }
        LOG.info("can not find the cluster with name:" + name);
        return null;
    }

    // change 20150812161819 to 2015-08-12 16:18
    public static String prettyDateTime(String time) {
        if (time.length() < 12) return time;
        StringBuilder sb = new StringBuilder();
        sb.append(time.substring(0, 4)).append("-").append(time.substring(4, 6)).append("-").append(time.substring(6, 8));
        sb.append(" ").append(time.substring(8, 10)).append(":").append(time.substring(10, 12));
        return sb.toString();
    }

    public static String prettyFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[]{"B", "KB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    //return the default value instead of throw an exception
    public static Long parseLong(String s, long defaultValue) {
        try {
            Long value = Long.parseLong(s);
            return value;
        } catch (NumberFormatException e) {
            //do nothing
        }
        return defaultValue;
    }

    // change 1441162320000 to "09-02 15:20"
    public static String parseDayTime(long ts) {
        return dayFormat.format(new Date(ts));
    }

    public static String parseDateTime(long ts) {
        return dateFormat.format(new Date(ts));
    }

    public static Map<String, Object> getNimbusConf(String clusterName) {
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            String jsonConf = client.getClient().getNimbusConf();
            Map<String, Object> nimbusConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);
            return nimbusConf;
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            return UIUtils.readUiConfig();
        }
    }

    public static Map<String, Object> getTopologyConf(String clusterName, String topologyId) {
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            String jsonConf = client.getClient().getTopologyConf(topologyId);
            Map<String, Object> topologyConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);
            return topologyConf;
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            return getNimbusConf(clusterName);
        }
    }


    public static Response getSupervisorConf(String host, int port) {
        String proxyUrl = "http://%s:%s/logview?cmd=showConf";
        String url = String.format(proxyUrl, host, port);

        return getHttpResponse(url);
    }

    public static Response getJStack(String host, int port, int workerPort) {
        String proxyUrl = "http://%s:%s/logview?cmd=jstack&workerPort=%s";
        String url = String.format(proxyUrl, host, port, workerPort);

        return getHttpResponse(url);
    }

    public static Response getFiles(String host, int port, String dir) {
        String url = String.format("http://%s:%s/logview?cmd=listDir&dir=%s", host, port, dir);
        return getHttpResponse(url);
    }

    public static Response getLog(String host, int port, String file, long position) {
        String url = String.format("http://%s:%s/logview?cmd=showLog&log=%s", host, port, file);
        if (position >= 0) {
            url += "&pos=" + position;
        }
        return getHttpResponse(url);
    }

    public static Response getHttpResponse(String url) {
        Response res;
        try {
            // 1. proxy call the task host log view service
            HttpClient client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            HttpResponse response = client.execute(post);
            int status = response.getStatusLine().getStatusCode();
            String data = EntityUtils.toString(response.getEntity());
            res = new Response(status, data);
        } catch (Exception e) {
            res = new Response(-1, e.getMessage());
        }
        return res;
    }

    /**
     * gets a topology tree & nodes for topology graph utilization
     *
     * @param stormTopology jstorm sys-topology
     * @return a TopologyGraph
     */
    public static TopologyGraph getTopologyGraph(StormTopology stormTopology, List<MetricInfo> componentMetrics) {
        Map<String, Bolt> bolts = stormTopology.get_bolts();
        Map<String, SpoutSpec> spouts = stormTopology.get_spouts();

        //remove system bolts
        String[] remove_ids = new String[]{"__acker", "__system", "__topology_master"};
        for (String id : remove_ids) {
            bolts.remove(id);
        }

        //<id, node>
        Map<String, TopologyNode> nodes = Maps.newHashMap();
        //<from:to,edge>
        Map<String, TopologyEdge> edges = Maps.newHashMap();


        List<TreeNode> roots = Lists.newArrayList();            //this is used to get tree depth
        Map<String, TreeNode> linkMap = Maps.newHashMap();      //this is used to get tree depth
        // init the nodes
        for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            String componentId = entry.getKey();
            nodes.put(componentId, new TopologyNode(componentId, componentId, true));

            TreeNode node = new TreeNode(componentId);
            roots.add(node);
            linkMap.put(componentId, node);
        }
        for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
            String componentId = entry.getKey();
            nodes.put(componentId, new TopologyNode(componentId, componentId, false));
            linkMap.put(componentId, new TreeNode(componentId));
        }

        //init the edges
        int edgeId = 1;
        for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
            String componentId = entry.getKey();
            Bolt bolt = entry.getValue();
            TreeNode node = linkMap.get(componentId);
            for (Map.Entry<GlobalStreamId, Grouping> input : bolt.get_common().get_inputs().entrySet()) {
                GlobalStreamId streamId = input.getKey();
                String src = streamId.get_componentId();
                if (nodes.containsKey(src)) {
                    TopologyEdge edge = new TopologyEdge(src, componentId, edgeId++);
                    edges.put(edge.getKey(), edge);
                    // put into linkMap
                    linkMap.get(src).addChild(node);
                    node.addSource(src);
                    node.addParent(linkMap.get(src));
                }
            }
        }

        //calculate whether has circle
        boolean isFixed = false;
        while (!isFixed) {
            isFixed = true;
            for (TreeNode node : linkMap.values()) {
                for (TreeNode parent : node.getParents()) {
                    if (!node.addSources(parent.getSources())) {
                        isFixed = false;
                    }
                }
            }
        }


        // fill value to edges
        fillTPSValue2Edge(componentMetrics, edges);
        fillTLCValue2Edge(componentMetrics, edges);


        // fill value to nodes
        fillValue2Node(componentMetrics, nodes);

        // calculate notes' depth
        int maxDepth = bfsDepth(roots);

        // set nodes level & get max breadth
        Map<Integer, Integer> counter = Maps.newHashMap();
        int maxBreadth = 1;
        for (Map.Entry<String, TreeNode> entry : linkMap.entrySet()) {
            int layer = entry.getValue().getLayer();
            nodes.get(entry.getKey()).setLevel(layer);
            int breadth = 1;
            if (counter.containsKey(layer)) {
                breadth = counter.get(layer) + 1;
            }
            counter.put(layer, breadth);
            if (maxBreadth < breadth) {
                maxBreadth = breadth;
            }
        }

        //adjust graph for components in one line
        String PREFIX = "__adjust_prefix_";
        int count = 1;
        for (TreeNode node : linkMap.values()) {
            int layer = node.getLayer();
            node.setBreadth(counter.get(layer));
        }
        for (TreeNode tree : linkMap.values()) {
            if (isInOneLine(tree.getChildren())) {
                String id = PREFIX + count++;
                TopologyNode node = new TopologyNode(id, id, false);
                node.setLevel(tree.getLayer() + 1);
                node.setIsHidden(true);
                nodes.put(id, node);

                TreeNode furthest = getFurthestNode(tree.getChildren());

                TopologyEdge toEdge = new TopologyEdge(id, furthest.getComponentId(), edgeId++);
                toEdge.setIsHidden(true);
                edges.put(toEdge.getKey(), toEdge);

                TopologyEdge fromEdge = new TopologyEdge(tree.getComponentId(), id, edgeId++);
                fromEdge.setIsHidden(true);
                edges.put(fromEdge.getKey(), fromEdge);
            }
        }

        TopologyGraph graph = new TopologyGraph(Lists.newArrayList(nodes.values()), Lists.newArrayList(edges.values()));

        graph.setDepth(maxDepth);
        graph.setBreadth(maxBreadth);

        //create graph object
        return graph;
    }

    private static TreeNode getFurthestNode(List<TreeNode> children) {
        TreeNode the = children.get(0);
        for (TreeNode node : children) {
            if (node.getLayer() > the.getLayer()) {
                the = node;
            }
        }
        return the;
    }

    private static boolean isInOneLine(List<TreeNode> children) {
        if (children.size() <= 1) {
            return false;
        }
        for (TreeNode node : children) {
            if (node.getBreadth() != 1) {
                return false;
            }
        }
        return true;
    }


    // fill emitted num to nodes
    private static void fillValue2Node(List<MetricInfo> componentMetrics, Map<String, TopologyNode> nodes) {
        String NODE_DIM = MetricDef.EMMITTED_NUM;
        List<String> FILTER = Arrays.asList(MetricDef.EMMITTED_NUM, MetricDef.SEND_TPS, MetricDef.RECV_TPS);
        for (MetricInfo info : componentMetrics) {
            if (info == null) continue;
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);
                String compName = UIMetricUtils.extractComponentName(split_name);

                TopologyNode node = nodes.get(compName);
                if (node != null && FILTER.contains(metricName)) {
                    for (Map.Entry<Integer, MetricSnapshot> winData : metric.getValue().entrySet()) {
                        node.putMapValue(metricName, winData.getKey(),
                                UIMetricUtils.getMetricValue(winData.getValue()));
                    }
                }

                if (metricName == null || !metricName.equals(NODE_DIM)) {
                    continue;
                }

                //get 60 window metric
                MetricSnapshot snapshot = metric.getValue().get(AsmWindow.M1_WINDOW);

                if (node != null) {
                    node.setValue(snapshot.get_longValue());
                    nodes.get(compName).setTitle("Emitted: " + UIMetricUtils.getMetricValue(snapshot));
                }
            }
        }
    }

    //fill tuple life cycle time to edges
    private static void fillTLCValue2Edge(List<MetricInfo> componentMetrics, Map<String, TopologyEdge> edges) {
        String EDGE_DIM = "." + MetricDef.TUPLE_LIEF_CYCLE;
        for (MetricInfo info : componentMetrics) {
            if (info == null) continue;
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                // only handle with `.TupleLifeCycle` metrics
                if (metricName == null || !metricName.contains(EDGE_DIM)) {
                    continue;
                }
                String componentId = UIMetricUtils.extractComponentName(split_name);
                String src = metricName.split("\\.")[0];
                String key = src + ":" + componentId;

                //get 60 window metric
                MetricSnapshot snapshot = metric.getValue().get(AsmWindow.M1_WINDOW);
                TopologyEdge edge = edges.get(key);
                if (edge != null) {
                    double value = snapshot.get_mean() / 1000;
                    edge.setCycleValue(value);
                    edge.appendTitle("TupleLifeCycle: " +
                            UIMetricUtils.format.format(value) + "ms");

                    for (Map.Entry<Integer, MetricSnapshot> winData : metric.getValue().entrySet()) {
                        // put the tuple life cycle time , unit is ms
                        double v = winData.getValue().get_mean() / 1000;
                        edge.putMapValue(MetricDef.TUPLE_LIEF_CYCLE + "(ms)", winData.getKey(),
                                UIMetricUtils.format.format(v));
                    }
                }
            }
        }
    }

    private static void fillTPSValue2Edge(List<MetricInfo> componentMetrics, Map<String, TopologyEdge> edges) {
        String EDGE_DIM = "." + MetricDef.RECV_TPS;
        for (MetricInfo info : componentMetrics) {
            if (info == null) continue;
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                // only handle with `.RecvTps` metrics
                if (metricName == null || !metricName.contains(EDGE_DIM)) {
                    continue;
                }
                String componentId = UIMetricUtils.extractComponentName(split_name);
                String src = metricName.split("\\.")[0];
                String key = src + ":" + componentId;

                //get 60 window metric
                MetricSnapshot snapshot = metric.getValue().get(AsmWindow.M1_WINDOW);
                TopologyEdge edge = edges.get(key);
                if (edge != null) {
                    edge.setValue(snapshot.get_m1());
                    edge.setTitle("TPS: " + UIMetricUtils.format.format(edges.get(key).getValue()));

                    for (Map.Entry<Integer, MetricSnapshot> winData : metric.getValue().entrySet()) {
                        edge.putMapValue("TPS", winData.getKey(),
                                UIMetricUtils.getMetricValue(winData.getValue()));
                    }
                }
            }
        }
    }

    private static int bfsDepth(List<TreeNode> roots) {
        LinkedList<TreeNode> queue = Lists.newLinkedList();
        for (TreeNode root : roots) {
            root.setLayer(0);
            queue.push(root);
        }

        int depth = 0;
        while (!queue.isEmpty()) {
            TreeNode cur = queue.poll();
            if (cur.getLayer() > depth) {
                depth = cur.getLayer();
            }
            for (TreeNode n : cur.getChildren()) {

                int newLayer = cur.getLayer() + 1;
                if (!n.isVisited() || n.getLayer() < newLayer) {
                    if (!n.inCircle(cur)) {
                        //n and cur is not in loop
                        n.setLayer(newLayer);
                        if (!queue.contains(n)) {
                            queue.push(n);
                        }
                    } else if (n.addLoopNode(cur)) {
                        // loop nodes only set layer once.
                        n.setLayer(newLayer);
                    }
                }

            }
        }
        return depth;
    }

    public static List<ChartSeries> getChartSeries(List<MetricInfo> infos, int window) {
        Map<String, ChartSeries> chartMap = Maps.newHashMap();
        for (MetricInfo info : infos) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);
                ChartSeries series;
                if (chartMap.containsKey(metricName)) {
                    series = chartMap.get(metricName);
                } else {
                    series = new ChartSeries(metricName);
                    chartMap.put(metricName, series);
                }

                Number number = UIMetricUtils.getMetricNumberValue(snapshot);
                series.addData(number);

                if (metricName != null && metricName.equals(MetricDef.MEMORY_USED)) {
                    series.addLabel(UIUtils.prettyFileSize(number.longValue()));
                } else {
                    series.addLabel(UIMetricUtils.getMetricValue(snapshot));
                }

                series.addCategory(UIUtils.parseDayTime(snapshot.get_ts()));
            }
        }

        return Lists.newArrayList(chartMap.values());
    }


    public static void addWindowAttribute(ModelMap model, int window) {
        String DEFAULT = "label-default";
        String PRIMARY = "label-primary";
        String cls_60 = DEFAULT;
        String cls_600 = DEFAULT;
        String cls_7200 = DEFAULT;
        String cls_86400 = DEFAULT;
        switch (window) {
            case 60:
                cls_60 = PRIMARY;
                break;
            case 600:
                cls_600 = PRIMARY;
                break;
            case 7200:
                cls_7200 = PRIMARY;
                break;
            case 86400:
                cls_86400 = PRIMARY;
                break;
        }
        model.addAttribute("cls_60", cls_60);
        model.addAttribute("cls_600", cls_600);
        model.addAttribute("cls_7200", cls_7200);
        model.addAttribute("cls_86400", cls_86400);
    }

    public static void addErrorAttribute(ModelMap model, Exception e) {
        String errMsg = JStormUtils.getErrorInfo(e);
        String err = "!!!!!!!!!!!!!!!! Error:" + e.getMessage() + " !!!!!!!!!\r\n\r\n" + errMsg;
        model.addAttribute("error", err);
    }

    public static void addTitleAttribute(ModelMap model, String prefix) {
        String displayName;
        if (StringUtils.isBlank(prefix)) {
            displayName = UIDef.APP_NAME;
        } else {
            displayName = prefix + " - " + UIDef.APP_NAME;
        }
        model.addAttribute("title", displayName);
        model.addAttribute("PAGE_MAX", UIDef.PAGE_MAX);
    }
}