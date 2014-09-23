package backtype.storm.command;

import java.util.Map;
import java.security.InvalidParameterException;

import backtype.storm.generated.MonitorOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Monitor topology
 * 
 * @author Basti
 * 
 */
public class metrics_monitor {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length <= 1) {
			throw new InvalidParameterException("Should input topology name and enable flag");
		}

		String topologyName = args[0];

		NimbusClient client = null;
		try {

			Map conf = Utils.readStormConfig();
			client = NimbusClient.getConfiguredClient(conf);

			boolean isEnable = Boolean.valueOf(args[1]).booleanValue();

			MonitorOptions options = new MonitorOptions();
			options.set_isEnable(isEnable);

			client.getClient().metricMonitor(topologyName, options);

			String str = (isEnable) ? "enable" : "disable";
			System.out.println("Successfully submit command to " + str
					+ " the monitor of " + topologyName);
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
