package com.alibaba.jstorm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;


public class KafkaSpoutConfig implements Serializable {

	
	private static final long serialVersionUID = 1L;

	public List<Host> brokers;
	public int numPartitions;
	public String topic;
	public String zkRoot;
	
	public List<Host> zkServers;
	
	public int fetchMaxBytes = 256*1024;
	public int fetchWaitMaxMs = 10000;
    public int socketTimeoutMs = 30 * 1000;
    public int socketReceiveBufferBytes = 64*1024;
    public long startOffsetTime = -1;
    public boolean fromBeginning = false;
    public String clientId;
    public boolean resetOffsetIfOutOfRange = false;
    public long offsetUpdateIntervalMs=2000;
    private Properties properties = null;
    private Map stormConf;
    public int batchSendCount = 1;
    
    public KafkaSpoutConfig() {
    }
    
    public KafkaSpoutConfig(Properties properties) {
        this.properties = properties;
    }
    
    public void configure(Map conf) {
        this.stormConf = conf;
        topic = getConfig("kafka.topic", "jstorm");
        zkRoot = getConfig("storm.zookeeper.root", "/jstorm");
        
        String zkHosts = getConfig("kafka.zookeeper.hosts", "127.0.0.1:2181");
        zkServers = convertHosts(zkHosts, 2181);
        String brokerHosts = getConfig("kafka.broker.hosts", "127.0.0.1:9092");
        brokers = convertHosts(brokerHosts, 9092);
        
        numPartitions = JStormUtils.parseInt(getConfig("kafka.broker.partitions"), 1);
        fetchMaxBytes = JStormUtils.parseInt(getConfig("kafka.fetch.max.bytes"), 256*1024);
        fetchWaitMaxMs = JStormUtils.parseInt(getConfig("kafka.fetch.wait.max.ms"), 10000);
        socketTimeoutMs = JStormUtils.parseInt(getConfig("kafka.socket.timeout.ms"), 30 * 1000);
        socketReceiveBufferBytes = JStormUtils.parseInt(getConfig("kafka.socket.receive.buffer.bytes"), 64*1024);
        fromBeginning = JStormUtils.parseBoolean(getConfig("kafka.fetch.from.beginning"), false);
        startOffsetTime = JStormUtils.parseInt(getConfig("kafka.start.offset.time"), -1);
        offsetUpdateIntervalMs = JStormUtils.parseInt(getConfig("kafka.offset.update.interval.ms"), 2000);
        clientId = getConfig("kafka.client.id", "jstorm");
        batchSendCount = JStormUtils.parseInt(getConfig("kafka.spout.batch.send.count"), 1);
    }
    
    
      private String getConfig(String key) {
          return getConfig(key, null);
      }
        
	  private String getConfig(String key, String defaultValue) {
	      if(properties!=null && properties.containsKey(key)) {
	          return properties.getProperty(key);
	      }else if(stormConf.containsKey(key)) {
	          return String.valueOf(stormConf.get(key));
	      }else {
	          return defaultValue;
	      }
	  }

	
	public  List<Host> convertHosts(String hosts, int defaultPort) {
	    List<Host> hostList = new ArrayList<Host>();
	    String[] hostArr = hosts.split(",");
        for (String s : hostArr) {
            Host host;
            String[] spec = s.split(":");
            if (spec.length == 1) {
                host = new Host(spec[0],defaultPort);
            } else if (spec.length == 2) {
                host = new Host(spec[0], JStormUtils.parseInt(spec[1]));
            } else {
                throw new IllegalArgumentException("Invalid host specification: " + s);
            }
            hostList.add(host);
        }
        return hostList;
    }
	

	public List<Host> getHosts() {
		return brokers;
	}

	public void setHosts(List<Host> hosts) {
		this.brokers = hosts;
	}

	public int getPartitionsPerBroker() {
		return numPartitions;
	}

	public void setPartitionsPerBroker(int partitionsPerBroker) {
		this.numPartitions = partitionsPerBroker;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	
}
