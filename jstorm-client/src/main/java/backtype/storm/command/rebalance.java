package backtype.storm.command;

import java.security.InvalidParameterException;
import java.util.Map;

import backtype.storm.generated.RebalanceOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Active topology
 * 
 * @author longda
 * 
 */
public class rebalance {

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

			if (args.length == 1) {

				client.getClient().rebalance(topologyName, null);
			} else {
				int delaySeconds = Integer.parseInt(args[1]);

				RebalanceOptions options = new RebalanceOptions();
				options.set_wait_secs(delaySeconds);

				client.getClient().rebalance(topologyName, options);
			}

			System.out.println("Successfully submit command rebalance "
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
