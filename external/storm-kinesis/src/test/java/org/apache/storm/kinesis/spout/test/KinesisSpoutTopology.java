package org.apache.storm.kinesis.spout.test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kinesis.spout.CredentialsProviderChain;
import org.apache.storm.kinesis.spout.ExponentialBackoffRetrier;
import org.apache.storm.kinesis.spout.KinesisConnectionInfo;
import org.apache.storm.kinesis.spout.KinesisSpout;
import org.apache.storm.kinesis.spout.RecordToTupleMapper;
import org.apache.storm.kinesis.spout.ZkInfo;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Date;

public class KinesisSpoutTopology {
    public static void main (String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String topologyName = args[0];
        RecordToTupleMapper recordToTupleMapper = new TestRecordToTupleMapper();
        KinesisConnectionInfo kinesisConnectionInfo = new KinesisConnectionInfo(new CredentialsProviderChain(), new ClientConfiguration(), Regions.US_WEST_2,
                1000);
        org.apache.storm.kinesis.spout.Config config = new org.apache.storm.kinesis.spout.Config(args[1], ShardIteratorType.TRIM_HORIZON,
                recordToTupleMapper, new Date(), new ExponentialBackoffRetrier(), new ZkInfo(), kinesisConnectionInfo, 10000L);
        KinesisSpout kinesisSpout = new KinesisSpout(config);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", kinesisSpout, 3);
        topologyBuilder.setBolt("bolt", new KinesisBoltTest(), 1).shuffleGrouping("spout");
        Config topologyConfig = new Config();
        topologyConfig.setDebug(true);
        topologyConfig.setNumWorkers(3);
        StormSubmitter.submitTopology(topologyName, topologyConfig, topologyBuilder.createTopology());
    }
}