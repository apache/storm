package backtype.storm.command;

import java.security.InvalidParameterException;
import java.util.Map;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Activate topology
 * 
 * @author longda
 * 
 */
public class activate {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length == 0) {
			throw new InvalidParameterException("Should input topology name");
		}

		String topologyName = args[0];

		NimbusClient client = null;
		try {

			Map conf = Utils.readStormConfig();
			client = NimbusClient.getConfiguredClient(conf);

			client.getClient().activate(topologyName);

			System.out.println("Successfully submit command activate "
					+ topologyName);
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
