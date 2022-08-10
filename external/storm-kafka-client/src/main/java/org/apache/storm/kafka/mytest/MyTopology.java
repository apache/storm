package org.apache.storm.kafka.mytest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * @author ：panda_z
 * @date ：Created in 2022/8/8
 * @description: kafkaspout test topology
 * @version: 1.0
 */
public class MyTopology {

    private static final String VERSION = "1.0";
    private static final String SPOUTID = "MyKafkaSpout-" + VERSION;
    private static final String OUTBOLT = "OutBolt- " + VERSION;

    public static void main(String[] args) {
        boolean local = true;
        LocalCluster cluster = null;
        Config topologyConfig = getMyTopologyConfig();
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        getMyTopology(topologyBuilder);
        if(local) {
            try {
                cluster = new LocalCluster();
                cluster.submitTopology(MyTopology.class.getSimpleName(), topologyConfig, topologyBuilder.createTopology());
                Utils.sleep(600000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cluster.shutdown();
            }
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar(MyTopology.class.getSimpleName(), topologyConfig, topologyBuilder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * description: build topology
     * Params: TopologyBuilder topologyBuilder
     */
    private static void getMyTopology(TopologyBuilder topologyBuilder) {
        KafkaSpout kafkaSpout = new MyKafkaSpout().getKafkaSpout();
        topologyBuilder.setSpout(SPOUTID, kafkaSpout, 1);
        topologyBuilder.setBolt(OUTBOLT, new OutBolt(), 1).shuffleGrouping(SPOUTID);
    }

    /**
     * description: get the config of MyTopology
     * @return Config config
     */
    private static Config getMyTopologyConfig() {
        Config config = new Config();
        config.setNumAckers(1);
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(30);

        return config;
    }

}