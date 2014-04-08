package com.alibaba.jstorm.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormConfig;

/**
 * storm utils
 * 
 * 
 * @author yannian/Longda/Xin.Zhou/Xin.Li
 * 
 */
public class JStormServerUtils {

	private static final Logger LOG = Logger.getLogger(JStormServerUtils.class);

	public static RunnableCallback getDefaultKillfn() {

		return new AsyncLoopDefaultKill();
	}

	public static TreeMap<Integer, Integer> integer_divided(int sum,
			int num_pieces) {
		return Utils.integerDivided(sum, num_pieces);
	}

	public static void sleep_secs(long secs) throws InterruptedException {
		Time.sleep(1000 * secs);
	}

	public static <K, V> HashMap<K, V> filter_val(RunnableCallback fn,
			Map<K, V> amap) {
		HashMap<K, V> rtn = new HashMap<K, V>();

		for (Entry<K, V> entry : amap.entrySet()) {
			V value = entry.getValue();
			Object result = fn.execute(value);

			if (result == (Boolean) true) {
				rtn.put(entry.getKey(), value);
			}
		}
		return rtn;
	}

	public static void downloadCodeFromMaster(Map conf, String localRoot,
			String masterCodeDir) throws IOException, TException {
		FileUtils.forceMkdir(new File(localRoot));

		String localStormjarPath = StormConfig.stormjar_path(localRoot);
		String masterStormjarPath = StormConfig.stormjar_path(masterCodeDir);
		Utils.downloadFromMaster(conf, masterStormjarPath, localStormjarPath);

		String localStormcodePath = StormConfig.stormcode_path(localRoot);
		String masterStormcodePath = StormConfig.stormcode_path(masterCodeDir);
		Utils.downloadFromMaster(conf, masterStormcodePath, localStormcodePath);

		String localStormConfPath = StormConfig.sotrmconf_path(localRoot);
		String masterStormConfPath = StormConfig.sotrmconf_path(masterCodeDir);
		Utils.downloadFromMaster(conf, masterStormConfPath, localStormConfPath);
	}

}
