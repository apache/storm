package com.alipay.dw.jstorm.example.sequence;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.Tool;
import backtype.storm.ToolRunner;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import com.alipay.dw.jstorm.example.sequence.bolt.MergeRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.PairCount;
import com.alipay.dw.jstorm.example.sequence.bolt.SplitRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.TotalCount;
import com.alipay.dw.jstorm.example.sequence.spout.SequenceSpout;


public class SequenceTopologyTool extends Tool{

	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";

	public  void SetLocalTopology() throws Exception {
		Config conf = getConf();

		StormTopology topology = buildTopology();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SplitMerge", conf, topology);
		Thread.sleep(60000);
		cluster.shutdown();
	}

	@Override
	public int run(String[] args) throws Exception {
		Config conf = getConf();
		if (conf.get(Config.STORM_CLUSTER_MODE).equals("rpc")) {
			SetDPRCTopology();
			return 0;
		} else if (conf.get(Config.STORM_CLUSTER_MODE).equals("local")) {
			SetLocalTopology();
			return 0;
		}
		else
		{ 
		   SetRemoteTopology();
		   return 0;
		} 
	}
	public StormTopology buildTopology()
	{
		Config conf = getConf();
		TopologyBuilder builder = new TopologyBuilder();

		int spout_Parallelism_hint = JStormUtils.parseInt(
				conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
		int bolt_Parallelism_hint = JStormUtils.parseInt(
				conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);

		builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
				new SequenceSpout(), spout_Parallelism_hint);

		boolean isEnableSplit = JStormUtils.parseBoolean(
				conf.get("enable.split"), false);

		if (isEnableSplit == false) {
			builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME,
					new TotalCount(), bolt_Parallelism_hint).localFirstGrouping(
					SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
		} else {

			builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME,
					new SplitRecord(), bolt_Parallelism_hint)
					.localOrShuffleGrouping(
							SequenceTopologyDef.SEQUENCE_SPOUT_NAME);

			builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME,
					new PairCount(), bolt_Parallelism_hint).shuffleGrouping(
					SequenceTopologyDef.SPLIT_BOLT_NAME,
					SequenceTopologyDef.TRADE_STREAM_ID);
			builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME,
					new PairCount(), bolt_Parallelism_hint).shuffleGrouping(
					SequenceTopologyDef.SPLIT_BOLT_NAME,
					SequenceTopologyDef.CUSTOMER_STREAM_ID);

			builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME,
					new MergeRecord(), bolt_Parallelism_hint)
					.fieldsGrouping(SequenceTopologyDef.TRADE_BOLT_NAME,
							new Fields("ID"))
					.fieldsGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME,
							new Fields("ID"));

			builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME,
					new TotalCount(), bolt_Parallelism_hint).noneGrouping(
					SequenceTopologyDef.MERGE_BOLT_NAME);
		}

		boolean kryoEnable = JStormUtils.parseBoolean(conf.get("kryo.enable"),
				false);
		if (kryoEnable == true) {
			System.out.println("Use Kryo ");
			boolean useJavaSer = JStormUtils.parseBoolean(
					conf.get("fall.back.on.java.serialization"), true);

			Config.setFallBackOnJavaSerialization(conf, useJavaSer);

			Config.registerSerialization(conf, TradeCustomer.class);
			Config.registerSerialization(conf, Pair.class);
		}
		int ackerNum = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
		Config.setNumAckers(conf, ackerNum);

		int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS),
				20);
		conf.put(Config.TOPOLOGY_WORKERS, workerNum);

		return  builder.createTopology();	
	}	
	
	public  void SetRemoteTopology() throws AlreadyAliveException,
			InvalidTopologyException, TopologyAssignException {
		 Config conf = getConf();
		StormTopology topology = buildTopology();

		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
		if (streamName == null) {
			streamName = "SequenceTest";
		}

		if (streamName.contains("zeromq")) {
			conf.put(Config.STORM_MESSAGING_TRANSPORT,
					"com.alibaba.jstorm.message.zeroMq.MQContext");

		} else {
			conf.put(Config.STORM_MESSAGING_TRANSPORT,
					"com.alibaba.jstorm.message.netty.NettyContext");
		}

		StormSubmitter.submitTopology(streamName, conf,topology);
		
	}

	public  void SetDPRCTopology() throws AlreadyAliveException,
			InvalidTopologyException, TopologyAssignException {
//		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
//				"exclamation");
//
//		builder.addBolt(new TotalCount(), 3);
//
//		Config conf = new Config();
//
//		conf.setNumWorkers(3);
//		StormSubmitter.submitTopology("rpc", conf,
//				builder.createRemoteTopology());
		System.out.println("Please refer to com.alipay.dw.jstorm.example.drpc.ReachTopology");
	}
	public static void main(String[] args) throws Exception {
	//	System.out.println("bj"+args[0]);
//		System.out.println(args[0]+args[1]);
	//	GenericOptionsParser.printGenericCommandUsage(System.out);
	    ToolRunner.run(new SequenceTopologyTool(), args);	
	}
	
}
