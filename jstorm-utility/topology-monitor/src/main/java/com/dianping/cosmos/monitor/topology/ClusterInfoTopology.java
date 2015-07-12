package com.dianping.cosmos.monitor.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class ClusterInfoTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setBolt("ClusterInfo", new ClusterInfoBolt(), 1);
        Config conf = new Config();
        conf.setNumWorkers(1);
        
        StormSubmitter.submitTopology("ClusterMonitor", conf, builder.createTopology());

    }
}
