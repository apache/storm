package com.alipay.dw.jstorm.example.batch;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;

public class SimpleBatchTopology {

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

	public static TopologyBuilder SetBuilder() {
		BatchTopologyBuilder topologyBuilder = new BatchTopologyBuilder(
				topologyName);
		
		int spoutParallel = JStormUtils.parseInt(conf.get("topology.spout.parallel"), 1);

		BoltDeclarer boltDeclarer = topologyBuilder.setSpout("Spout",
				new SimpleSpout(), spoutParallel);

		int boltParallel = JStormUtils.parseInt(conf.get("topology.bolt.parallel"), 2);
		topologyBuilder.setBolt("Bolt", new SimpleBolt(), boltParallel).shuffleGrouping(
				"Spout");

		return topologyBuilder.getTopologyBuilder();
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
