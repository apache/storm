package com.alibaba.jstorm.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.utils.Utils;

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
	
	public static void createPid(String dir) throws Exception{
		File file = new File(dir);
		
		if (file.exists() == false) {
			file.mkdirs();
		}else if (file.isDirectory() == false) {
			throw new RuntimeException("pid dir:" + dir + " isn't directory");
		}
		
		String[] existPids = file.list();
		
		// touch pid before
		String pid = JStormUtils.process_pid();
		String pidPath = dir + File.separator + pid;
		PathUtils.touch(pidPath);
		LOG.info("Successfully touch pid  " + pidPath);
		
		
		for (String existPid : existPids) {
			try {
				JStormUtils.kill(Integer.valueOf(existPid));
				PathUtils.rmpath(dir + File.separator + existPid);
			}catch(Exception e) {
				LOG.warn(e.getMessage(), e);
			}
		}
		
		
	}
	
	public static void startTaobaoJvmMonitor() {
//		JmonitorBootstrap bootstrap = JmonitorBootstrap.getInstance();
//		bootstrap.start();
	}

}
