package com.alibaba.aloha.meta.example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.aloha.meta.MetaSpout;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * MonitorTopology
 * 
 * @author longda/zhiyuan.ls
 * 
 */
public class TestTopology {

	private static Logger LOG = Logger.getLogger(TestTopology.class);

	public static String WRITER_COMPONENT = "writer";

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}

		LoadConf(args[0]);

		TopologyBuilder builder = setupBuilder();

		submitTopology(builder);

	}

	private static TopologyBuilder setupBuilder() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		int writerParallel = JStormUtils.parseInt(
				conf.get("topology.writer.parallel"), 1);

		int spoutParallel = JStormUtils.parseInt(
				conf.get("topology.spout.parallel"), 1);

		builder.setSpout("MetaSpout", new MetaSpout(), spoutParallel);

		builder.setBolt(WRITER_COMPONENT, new WriterBolt(), writerParallel)
				.shuffleGrouping("MetaSpout");

		return builder;
	}

	private static void submitTopology(TopologyBuilder builder) {
		try {
			if (local_mode(conf)) {

				LocalCluster cluster = new LocalCluster();

				cluster.submitTopology(
						String.valueOf(conf.get("topology.name")), conf,
						builder.createTopology());

				Thread.sleep(200000);

				cluster.shutdown();
			} else {
				StormSubmitter.submitTopology(
						String.valueOf(conf.get("topology.name")), conf,
						builder.createTopology());
			}

		} catch (Exception e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}

	private static Map conf = new HashMap<Object, Object>();

	private static void LoadProperty(String prop) {
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
		} catch (Exception e1) {
			e1.printStackTrace();

			return;
		}

		conf.putAll(properties);
	}

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

		return;
	}

	private static void LoadConf(String arg) {
		if (arg.endsWith("yaml")) {
			LoadYaml(arg);
		} else {
			LoadProperty(arg);
		}
	}

	public static boolean local_mode(Map conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		if (mode != null) {
			if (mode.equals("local")) {
				return true;
			}
		}

		return false;

	}

}
