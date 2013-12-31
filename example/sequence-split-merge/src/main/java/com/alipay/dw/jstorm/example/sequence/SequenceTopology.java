package com.alipay.dw.jstorm.example.sequence;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alipay.dw.jstorm.example.sequence.bolt.MergeRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.PairCount;
import com.alipay.dw.jstorm.example.sequence.bolt.SplitRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.TotalCount;
import com.alipay.dw.jstorm.example.sequence.spout.SequenceSpout;

public class SequenceTopology {
	
	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology_spout_parallelism_hint";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "topology_bolt_parallelism_hint";
    
    public static void SetBuilder(TopologyBuilder builder, Map conf) {
        
    	int spout_Parallelism_hint = conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT) == null
    			? 1 : (Integer)conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT);
    	int bolt_Parallelism_hint = conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT) == null
    			? 1 : (Integer)conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT);
    	
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
        
//        builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(),
//        		bolt_Parallelism_hint).noneGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
        
<<<<<<< HEAD
        builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SplitRecord(),
        		2).shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
        
        builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(),
        		1).shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.TRADE_STREAM_ID);
        builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(),
        		1).shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.CUSTOMER_STREAM_ID);
        
        builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new MergeRecord(), 
        		2).fieldsGrouping(SequenceTopologyDef.TRADE_BOLT_NAME, new Fields("ID"))
        		.fieldsGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new Fields("ID"));
        
=======
//        builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SplitRecord(),
//        		2).shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
//        
//        builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(),
//        		1).shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.TRADE_STREAM_ID);
//        builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(),
//        		1).shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.CUSTOMER_STREAM_ID);
//        
//        builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new MergeRecord(), 
//        		2).fieldsGrouping(SequenceTopologyDef.TRADE_BOLT_NAME, new Fields("ID"))
//        		.fieldsGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new Fields("ID"));
//        
>>>>>>> 0.9.0
        builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(), 
        		2).noneGrouping(SequenceTopologyDef.MERGE_BOLT_NAME);
        
        conf.put(Config.TOPOLOGY_DEBUG, false);
        //        conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
        //        conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        
        Config.setNumAckers(conf, 1);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        //        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        //conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        conf.put(Config.TOPOLOGY_WORKERS, 20);
        
    }
    
    public static void SetLocalTopology() throws InterruptedException {
//                TopologyBuilder builder = new TopologyBuilder();
//                
//                Map conf = new HashMap();
//                
//                SetBuilder(builder, conf);
//                
//                LocalCluster cluster = new LocalCluster();
//                cluster.submitTopology("SplitMerge", conf, builder.createTopology());
//                
//                Thread.sleep(1000000);
//                
//                cluster.shutdown();
    }
    
    public static void SetRemoteTopology(String streamName, Integer spout_parallelism_hint, Integer bolt_parallelism_hint)
            throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException {
        TopologyBuilder builder = new TopologyBuilder();
        
        Map conf = new HashMap();
        conf.put(TOPOLOGY_SPOUT_PARALLELISM_HINT, spout_parallelism_hint);
        conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, bolt_parallelism_hint);
        
        SetBuilder(builder, conf);
        
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        if (streamName.contains("netty")) {
        	 conf.put(Config.STORM_MESSAGING_TRANSPORT, "com.alibaba.jstorm.message.netty.NettyContext");
             conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
             conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
             conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
             conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
             conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
             conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
        } else {
        	 conf.put(Config.STORM_MESSAGING_TRANSPORT, "com.alibaba.jstorm.message.zeroMq.MQContext");
        }
       
        
        StormSubmitter.submitTopology(streamName, conf,
                builder.createTopology());
        
    }
    
    public static void SetDPRCTopology() throws AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
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
        	//args: 0-topologyName, 1-spoutParallelism, 2-boltParallelism
        	Integer spout_parallelism_hint = null;
        	Integer bolt_parallelism_hint = null;
        	if (args.length > 1) {
        		spout_parallelism_hint = Integer.parseInt(args[1]);
	        	if (args.length > 2) {
	        		bolt_parallelism_hint = Integer.parseInt(args[2]);
	        	}
        	}
        	SetRemoteTopology(args[0], spout_parallelism_hint, bolt_parallelism_hint);
        }
        
    }
}
