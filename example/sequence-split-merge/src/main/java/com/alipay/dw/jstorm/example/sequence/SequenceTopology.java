package com.alipay.dw.jstorm.example.sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alipay.dw.jstorm.client.ConfigExtension;
import com.alipay.dw.jstorm.example.sequence.bolt.MergeRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.PairCount;
import com.alipay.dw.jstorm.example.sequence.bolt.SplitRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.TotalCount;
import com.alipay.dw.jstorm.example.sequence.spout.SequenceSpout;

public class SequenceTopology {
    
    public static void SetBuilder(TopologyBuilder builder, Map conf) {
        
        builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
                new SequenceSpout(), 1);
        
        //        builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SplitRecord(), 2).fieldsGrouping(
        //                SequenceTopologyDef.SEQUENCE_SPOUT_NAME, new Fields("ID"));
        //        
        //        builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        //                SequenceTopologyDef.SPLIT_BOLT_NAME, 
        //                SequenceTopologyDef.TRADE_STREAM_ID);
        //        
        //        builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1)
        //                .shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME,
        //                        SequenceTopologyDef.CUSTOMER_STREAM_ID);
        //        
        //        builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new MergeRecord(), 1)
        //                .shuffleGrouping(SequenceTopologyDef.TRADE_BOLT_NAME)
        //                .shuffleGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME);
        
        builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(),
                1).noneGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
        
        conf.put(Config.TOPOLOGY_DEBUG, false);
        //        conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
        //        conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        
        conf.put(Config.TOPOLOGY_ACKERS, 1);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        //        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        //conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        conf.put(Config.TOPOLOGY_WORKERS, 6);
        
    }
    
    public static void SetLocalTopology() throws InterruptedException {
        //        TopologyBuilder builder = new TopologyBuilder();
        //        
        //        Map conf = new HashMap();
        //        
        //        SetBuilder(builder, conf);
        //        
        //        LocalCluster cluster = new LocalCluster();
        //        cluster.submitTopology("SplitMerge", conf, builder.createTopology());
        //        
        //        Thread.sleep(1000000);
        //        
        //        cluster.shutdown();
    }
    
    public static void SetRemoteTopology(String streamName)
            throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        
        Map conf = new HashMap();
        
        SetBuilder(builder, conf);
        
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        StormSubmitter.submitTopology(streamName, conf,
                builder.createTopology());
        
    }
    
    public static void SetDPRCTopology() throws AlreadyAliveException,
            InvalidTopologyException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
                "exclamation");
        
        builder.addBolt(new TotalCount(), 3);
        
        Config conf = new Config();
        
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology("rpc", conf,
                builder.createRemoteTopology());
    }
    
    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            SetLocalTopology();
        }
        if (args[0] == "rpc") {
            SetDPRCTopology();
        } else {
            SetRemoteTopology(args[0]);
        }
        
    }
}
