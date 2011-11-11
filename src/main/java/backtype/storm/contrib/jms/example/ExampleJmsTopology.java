package backtype.storm.contrib.jms.example;

import javax.jms.Session;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class ExampleJmsTopology {
	public static final int JMS_SPOUT = 1;
	public static final int INTERMEDIATE_BOLT = 2;
	public static final int FINAL_BOLT = 3;

	public static void main(String[] args) throws Exception {

		// JMS Provider
		JmsProvider jmsProvider = new SpringJmsProvider(
				"jms-activemq.xml", "jmsConnectionFactory",
				"notificationQueue");

		// JMS Producer
		JmsTupleProducer producer = new JsonTupleProducer();

		// JMS Spout
		JmsSpout spout = new JmsSpout();
		spout.setJmsProvider(jmsProvider);
		spout.setJmsTupleProducer(producer);
		spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);

		TopologyBuilder builder = new TopologyBuilder();
		
		// spout with 5 parallel instances
		builder.setSpout(JMS_SPOUT, spout, 5);

		// intermediate bolt, subscribes to jms spout, anchors on tuples, and auto-acks
		builder.setBolt(INTERMEDIATE_BOLT,
				new GenericBolt("INTERMEDIATE_BOLT", true, true, new Fields("json")), 3).shuffleGrouping(
				JMS_SPOUT);

		// bolt that subscribes to the intermediate bolt, and auto-acks
		// messages.
		builder.setBolt(FINAL_BOLT, new GenericBolt("FINAL_BOLT", true, true), 3).shuffleGrouping(
				INTERMEDIATE_BOLT);

		Config conf = new Config();

		if (args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			conf.setDebug(true);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("storm-jms-example", conf, builder.createTopology());
			Utils.sleep(120000);
			cluster.killTopology("storm-jms-example");
			cluster.shutdown();
		}
	}

}
