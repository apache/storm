package com.alibaba.jstorm;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LoadConfig {
	public static final String TOPOLOGY_TYPE = "topology.type";
	
	private static Map LoadProperty(String prop) {
		Map ret = null;
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
			ret = new HashMap<Object, Object>();
			ret.putAll(properties);
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		return ret;
	}

	private static Map LoadYaml(String confPath) {
        Map ret = null;
		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);

			ret = (Map) yaml.load(stream);
			if (ret == null || ret.isEmpty() == true) {
				throw new RuntimeException("Failed to read config file");
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
	
	public static Map LoadConf(String arg) {
		Map ret = null;
		
		if (arg.endsWith("yaml")) {
			ret = LoadYaml(arg);
		} else {
			ret = LoadProperty(arg);
		}
		
		return ret;
	}
}