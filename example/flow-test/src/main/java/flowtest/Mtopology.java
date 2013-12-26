package flowtest;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class Mtopology {
	
	public static Logger LOG = Logger.getLogger(Mtopology.class);

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws TopologyAssignException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException {
		// TODO Auto-generated method stub
		Properties prop = null;
		try {
			prop = readConf(args[0]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(prop == null)
			return;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Mspout", new Mspout());
		IBasicBolt bolt = new IBasicBolt() {

			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public Map<String, Object> getComponentConfiguration() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void prepare(Map stormConf, TopologyContext context) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void execute(Tuple input, BasicOutputCollector collector) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void cleanup() {
				// TODO Auto-generated method stub
				
			}
			
		};
		InputDeclarer inputDeclarer = builder.setBolt("Mbolt", bolt, Integer.parseInt(prop.getProperty("parallelism_hint", "80")));
		inputDeclarer.directGrouping("Mspout");
		Config conf = new Config();
		conf.setNumWorkers(Integer.parseInt(prop.getProperty("numWorkers", "82")));
		conf.setNumAckers(1);
		StormSubmitter.submitTopology(prop.getProperty("topologyname", "Mtopology"), conf, builder.createTopology());
	}
	
	public static Properties readConf(String fileName) throws Exception{
		Properties prop = new Properties();
		
		FileInputStream fileInputStream = new FileInputStream(fileName);
		
		prop.load(fileInputStream);
		
		return prop;	
	}
	
}
