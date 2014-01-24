package com.alipay.dw.jstorm.transcation;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.dw.jstorm.example.TpsCounter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TransactionalGlobalCount {
	public static Logger LOG = LoggerFactory.getLogger(TransactionalGlobalCount.class);
	
	 public static final int PARTITION_TAKE_PER_BATCH = 3;
	  public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
	    put(0, new ArrayList<List<Object>>() {{
	      add(new Values("cat"));
	      add(new Values("dog"));
	      add(new Values("chicken"));
	      add(new Values("cat"));
	      add(new Values("dog"));
	      add(new Values("apple"));
	    }});
	    put(1, new ArrayList<List<Object>>() {{
	      add(new Values("cat"));
	      add(new Values("dog"));
	      add(new Values("apple"));
	      add(new Values("banana"));
	    }});
	    put(2, new ArrayList<List<Object>>() {{
	      add(new Values("cat"));
	      add(new Values("cat"));
	      add(new Values("cat"));
	      add(new Values("cat"));
	      add(new Values("cat"));
	      add(new Values("dog"));
	      add(new Values("dog"));
	      add(new Values("dog"));
	      add(new Values("dog"));
	    }});
	  }};

	  public static class Value {
	    int count = 0;
	    BigInteger txid;
	  }

	  public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	  public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

	  public static class BatchCount extends BaseBatchBolt {
	    Object _id;
	    BatchOutputCollector _collector;

	    int _count = 0;

	    @Override
	    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
	      _collector = collector;
	      _id = id;
	    }

	    @Override
	    public void execute(Tuple tuple) {
	      _count++;
	      LOG.info("---BatchCount.execute(), _count=" + _count);
	    }

	    @Override
	    public void finishBatch() {
	      _collector.emit(new Values(_id, _count));
	      LOG.info("---BatchCount.finishBatch(), _id=" + _id + ", _count=" + _count);
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("id", "count"));
	    }
	  }

	  public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
	    TransactionAttempt _attempt;
	    BatchOutputCollector _collector;

	    int _sum = 0;

	    @Override
	    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
	      _collector = collector;
	      _attempt = attempt;
	    }

	    @Override
	    public void execute(Tuple tuple) {
	      _sum += tuple.getInteger(1);
	      LOG.info("---UpdateGlobalCount.execute(), _sum=" + _sum);
	    }

	    @Override
	    public void finishBatch() {
	      Value val = DATABASE.get(GLOBAL_COUNT_KEY);
	      Value newval;
	      if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
	        newval = new Value();
	        newval.txid = _attempt.getTransactionId();
	        if (val == null) {
	          newval.count = _sum;
	        }
	        else {
	          newval.count = _sum + val.count;
	        }
	        DATABASE.put(GLOBAL_COUNT_KEY, newval);
	      }
	      else {
	        newval = val;
	      }
	      _collector.emit(new Values(_attempt, newval.count));
	      LOG.info("---UpdateGlobalCount.finishBatch(), _attempt=" + _attempt 
	    		  + ", newval=(" + newval.txid + "," + newval.count + ")");
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("id", "sum"));
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
	    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 2);
	    builder.setBolt("partial-count", new BatchCount(), 3).noneGrouping("spout");
	    builder.setBolt("sum", new UpdateGlobalCount(), 1).globalGrouping("partial-count");

//	    LocalCluster cluster = new LocalCluster();

	    Config config = new Config();
	    config.setDebug(true);
	    config.setMaxSpoutPending(3);
	    config.put(Config.TOPOLOGY_WORKERS, 9);
	    Config.setNumAckers(config, 0);
	    
	    StormSubmitter.submitTopology("global-count-topology", config, builder.buildTopology());

//	    Thread.sleep(3000);
//	    cluster.shutdown();
	  }
}
