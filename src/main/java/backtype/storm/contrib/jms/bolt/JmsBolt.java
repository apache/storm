package backtype.storm.contrib.jms.bolt;

import java.util.Map;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class JmsBolt implements IRichBolt {
	
	private JmsProvider jmsProvider;
	
	private JmsMessageProducer producer;
	
	private OutputCollector collector;
	
	public void setJmsProvider(JmsProvider provider){
		this.jmsProvider = provider;
	}


	@Override
	public void execute(Tuple input) {
		// write the tuple to a JMS destination...
		//input.
		

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

}
