package org.apache.storm.hbase.trident.mapstate;


import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class HBaseMapStateDemoTopology {
	

	private final static String TOPO_NAME = "hbasemapstate_test";
	private final static String NIMBUS_HOST = "192.168.1.100";
	
	private final static String TABLE_NAME = "hbasemapstate_test";
	private final static String FAMILY = "f1";
	private final static String QUALIFIER = "q1";


	public StormTopology buildTopology() {
		
		FixedBatchSpout spout =
                new FixedBatchSpout(new Fields("sentence"), 3, new Values(
                        "the cow jumped over the moon"), new Values(
                        "the man went to the store and bought some candy"), new Values(
                        "four score and seven years ago"),
                        new Values("how many apples can you eat"), new Values(
                                "to be or not to be the person too"));
		spout.setCycle(true);
		
		HBaseMapState.Options option = new HBaseMapState.Options();
		option.tableName = TABLE_NAME;
		option.columnFamily = FAMILY;
		option.mapMapper = new SimpleTridentHBaseMapMapper(QUALIFIER);

		TridentTopology topology = new TridentTopology();
        topology.newStream("wordsplit", spout).shuffle().
                each(new Fields("sentence"), new WordSplit(), new Fields("word")).
                groupBy(new Fields("word")).
                persistentAggregate(HBaseMapState.transactional(option), new Count(), new Fields("aggregates_words")).parallelismHint(1);

		return topology.build();
	}


	public static void main(String[] args) throws Exception {

		HBaseMapStateDemoTopology topology = new HBaseMapStateDemoTopology();
		Config config = new Config();

		if (args != null && args.length > 1) {
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(5);
			config.put(Config.NIMBUS_HOST, NIMBUS_HOST);
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(NIMBUS_HOST));
			StormSubmitter.submitTopology(TOPO_NAME, config, topology.buildTopology());
		} else {
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(2);
		
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPO_NAME, config, topology.buildTopology());

		}
	}
}
