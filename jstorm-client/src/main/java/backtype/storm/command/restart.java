package backtype.storm.command;

import java.security.InvalidParameterException;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSON;

import backtype.storm.generated.RebalanceOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Active topology
 * 
 * @author basti
 * 
 */
public class restart {	
	private static Map LoadProperty(String prop) {
		Map ret = new HashMap<Object, Object>();
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
			if (properties.size() == 0) {
				System.out.println("WARN: Config file is empty");
				return null;
			} else {
				ret.putAll(properties);
			}
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
			throw new RuntimeException(e.getMessage());
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		
		return ret;
	}

	private static Map LoadYaml(String confPath) {
		Map ret = new HashMap<Object, Object>();
		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);
			ret = (Map) yaml.load(stream);
			if (ret == null || ret.isEmpty() == true) {
				System.out.println("WARN: Config file is empty");
				return null;
			}
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + confPath);
			throw new RuntimeException("No config file");
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException("Failed to read config file");
		}

		return ret;
	}
	
	private static Map LoadConf(String arg) {
		Map ret = null;
		if (arg.endsWith("yaml")) {
			ret = LoadYaml(arg);
		} else {
			ret = LoadProperty(arg);
		}
		return ret;
	}

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
				client.getClient().restart(topologyName, null);
			} else {
				Map loadConf = LoadConf(args[1]);
				String jsonConf = JSON.toJSONString(loadConf);
				client.getClient().restart(topologyName, jsonConf);
			}

			System.out.println("Successfully submit command restart "
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
