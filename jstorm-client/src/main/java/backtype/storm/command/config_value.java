package backtype.storm.command;

import java.security.InvalidParameterException;
import java.util.Map;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Get configuration 
 * 
 * @author longda
 * 
 */
public class config_value {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length == 0) {
			throw new InvalidParameterException("Should input key name");
		}

		String key = args[0];

		Map conf = Utils.readStormConfig();

		System.out.print("VALUE: " + String.valueOf(conf.get(key)));
	}

}
