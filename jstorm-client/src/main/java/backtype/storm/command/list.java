package backtype.storm.command;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Activate topology
 * 
 * @author longda
 * 
 */
public class list {
	
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		NimbusClient client = null;
		try {

			Map conf = Utils.readStormConfig();
			client = NimbusClient.getConfiguredClient(conf);
			
			if (args.length > 0 && StringUtils.isBlank(args[0]) == false) {
				String topologyName = args[0];
				TopologyInfo info = client.getClient().getTopologyInfoByName(topologyName);
				
				System.out.println("Successfully get topology info \n"
						+ Utils.toPrettyJsonString(info));
			}else {
				ClusterSummary clusterSummary = client.getClient().getClusterInfo();
				
				System.out.println("Successfully get cluster info \n"
						+ Utils.toPrettyJsonString(clusterSummary));
			}

			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

}
