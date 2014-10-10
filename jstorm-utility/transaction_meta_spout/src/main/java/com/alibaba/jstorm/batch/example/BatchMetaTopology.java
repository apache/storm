package com.alibaba.jstorm.batch.example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alibaba.jstorm.batch.IBatchSpout;
import com.alibaba.jstorm.batch.meta.MetaSpoutConfig;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.local.LocalCluster;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeFormat;

public class BatchMetaTopology {
	private static String topologyName;

	private static Map conf;

	private static void LoadYaml(String confPath) {

		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);

			conf = (Map) yaml.load(stream);
			if (conf == null || conf.isEmpty() == true) {
				throw new RuntimeException("Failed to read config file");
			}

		} catch (FileNotFoundException e) {
			System.out.println("No such file " + confPath);
			throw new RuntimeException("No config file");
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException("Failed to read config file");
		}

		topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
		return;
	}

	public static MetaSpoutConfig getMetaSpoutConfig(Map conf) {
		String consumerGroup = (String) conf.get("meta.consumer.group");
		String topic = (String) conf.get("meta.topic");
		String nameServer = (String) conf.get("meta.nameserver");
		String subExpress = (String) conf.get("meta.subexpress");

		String startTimestampStr = (String) conf.get("meta.start.timestamp");
		Long startTimeStamp = null;
		if (startTimestampStr != null) {
			Date date = TimeFormat.getSecond(startTimestampStr);
			startTimeStamp = date.getTime();
		}

		int batchMessageNum = JStormUtils.parseInt(
				conf.get("meta.batch.message.num"), 1024);
		int maxFailTimes = JStormUtils.parseInt(
				conf.get("meta.max.fail.times"), 10);

		MetaSpoutConfig ret = new MetaSpoutConfig(consumerGroup, nameServer,
				topic, subExpress);
		ret.setStartTimeStamp(startTimeStamp);
		ret.setBatchMsgNum(batchMessageNum);
		ret.setMaxFailTimes(maxFailTimes);

		return ret;
	}

	public static TopologyBuilder SetBuilder() {

		BatchTopologyBuilder batchTopologyBuilder = new BatchTopologyBuilder(
				topologyName);

		MetaSpoutConfig metaSpoutConfig = getMetaSpoutConfig(conf);

		BoltDeclarer rebalanceDeclarer = batchTopologyBuilder.setBolt(
				BatchMetaRebalance.BOLT_NAME, new BatchMetaRebalance(), 1);

		IBatchSpout batchSpout = new BatchMetaSpout(metaSpoutConfig);
		int spoutParal = JStormUtils.parseInt(
				conf.get("topology.spout.parallel"), 1);
		BoltDeclarer spoutDeclarer = batchTopologyBuilder.setSpout(
				BatchMetaSpout.SPOUT_NAME, batchSpout, spoutParal);
		spoutDeclarer.allGrouping(BatchMetaRebalance.BOLT_NAME, 
				BatchMetaRebalance.REBALANCE_STREAM_ID);

		int boltParallel = JStormUtils.parseInt(
				conf.get("topology.bolt.parallel"), 1);
		BoltDeclarer transformDeclarer = batchTopologyBuilder.setBolt(
				TransformBolt.BOLT_NAME, new TransformBolt(), boltParallel);
		transformDeclarer.shuffleGrouping(BatchMetaSpout.SPOUT_NAME);

		BoltDeclarer countDeclarer = batchTopologyBuilder.setBolt(
				CountBolt.COUNT_BOLT_NAME, new CountBolt(), boltParallel);
		countDeclarer.shuffleGrouping(TransformBolt.BOLT_NAME);

		BoltDeclarer sumDeclarer = batchTopologyBuilder.setBolt(
				CountBolt.SUM_BOLT_NAME, new CountBolt(), boltParallel);
		sumDeclarer.shuffleGrouping(TransformBolt.BOLT_NAME);

		BoltDeclarer dbDeclarer = batchTopologyBuilder.setBolt(
				DBBolt.BOLT_NAME, new DBBolt(), 1);
		dbDeclarer.shuffleGrouping(CountBolt.COUNT_BOLT_NAME).shuffleGrouping(
				CountBolt.SUM_BOLT_NAME);

		return batchTopologyBuilder.getTopologyBuilder();
	}

	public static void SetLocalTopology() throws Exception {
		TopologyBuilder builder = SetBuilder();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, builder.createTopology());

		Thread.sleep(600000);

		cluster.shutdown();
	}

	public static void SetRemoteTopology() throws AlreadyAliveException,
			InvalidTopologyException, TopologyAssignException {

		TopologyBuilder builder = SetBuilder();

		StormSubmitter.submitTopology(topologyName, conf,
				builder.createTopology());

	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Please input parameters topology.yaml");
			System.exit(-1);
		}

		LoadYaml(args[0]);

		boolean isLocal = StormConfig.local_mode(conf);

		if (isLocal) {
			SetLocalTopology();
			return;
		} else {
			SetRemoteTopology();
		}

	}
}
