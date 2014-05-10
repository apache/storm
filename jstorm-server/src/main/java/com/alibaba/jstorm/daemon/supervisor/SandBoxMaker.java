package com.alibaba.jstorm.daemon.supervisor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;

/**
 * Right now generate java sandbox policy through template file
 * 
 * In the future, generating java sandbox policy will through hardcode
 * 
 * @author longda
 * @version
 */
public class SandBoxMaker {
	private static final Logger LOG = Logger.getLogger(SandBoxMaker.class);

	public static final String SANBOX_TEMPLATE_NAME = "sandbox.policy";
	
	public static final String JSTORM_HOME_KEY = "%JSTORM_HOME%";
	
	public static final String CLASS_PATH_KEY = "%CLASS_PATH%";
	
	public static final String LOCAL_DIR_KEY = "%JSTORM_LOCAL_DIR%";

	// this conf should only be Supervisor Conf
	private final Map conf;

	private final boolean isEnable;

	
	private final Map<String, String> replaceBaseMap = new HashMap<String, String>();

	public SandBoxMaker(Map conf) {
		this.conf = conf;

		isEnable = ConfigExtension.isJavaSandBoxEnable(conf);

		LOG.info("Java Sandbox Policy :" + String.valueOf(isEnable));

		String jstormHome = System.getProperty("jstorm.home");
		if (jstormHome == null) {
			jstormHome = "./";
		} 
		
		replaceBaseMap.put(JSTORM_HOME_KEY, jstormHome);
		
		replaceBaseMap.put(LOCAL_DIR_KEY, (String)conf.get(Config.STORM_LOCAL_DIR));
		
		
		LOG.info("JSTORM_HOME is " + jstormHome);
	}
	
	private String genClassPath(String classPathLine) {
		StringBuilder sb = new StringBuilder();
		
		String[] classPathes = classPathLine.split(":");
		for (String classpath: classPathes) {
			if (StringUtils.isBlank(classpath)) {
				continue;
			}
			
			File file = new File(classpath);
			if (file.isDirectory()) {
				sb.append(" permission java.io.FilePermission \"");
				sb.append(classpath).append(File.separator).append("**");
				sb.append("\", \"read\";\n");
			}else {
				sb.append(" permission java.io.FilePermission \"");
				sb.append(classpath);
				sb.append("\", \"read\";\n");
			}
			
		}
		
		return sb.toString();
	}
	
	private String replaceLine(String line, Map<String, String> replaceMap) {
		
		for (Entry<String, String>entry : replaceMap.entrySet()) {
			if (line.contains(CLASS_PATH_KEY)) {
				return genClassPath(entry.getValue());
			}else if (line.contains(entry.getKey())) {
				return line.replace(entry.getKey(), entry.getValue());
			}
		}
		
		return line;
	}
	
	public String generatePolicyFile(Map<String, String> replaceMap) throws IOException {
		// dynamic generate policy file, no static file
		String tmpPolicy = StormConfig.supervisorTmpDir(conf) + File.separator
				+ UUID.randomUUID().toString();

		InputStream inputStream = SandBoxMaker.class.getClassLoader()
				.getResourceAsStream(SANBOX_TEMPLATE_NAME);

		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
				tmpPolicy)));

		try {

			InputStreamReader inputReader = new InputStreamReader(inputStream);

			BufferedReader reader = new BufferedReader(new LineNumberReader(
					inputReader));

			String line = null;
			while ((line = reader.readLine()) != null) {
				String replaced = replaceLine(line, replaceMap);

				writer.println(replaced);
			}
			
			
			return tmpPolicy;
		} catch (Exception e) {
			LOG.error("Failed to generate policy file\n", e);
			throw new IOException(e);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (writer != null) {
				writer.close();
			}

		}
	}

	/**
	 * Generate command string
	 * 
	 * @param workerId
	 * @return
	 * @throws IOException
	 */
	public String sandboxPolicy(String workerId, Map<String, String> replaceMap) throws IOException {
		if (isEnable == false) {
			return "";
		}
		
		replaceMap.putAll(replaceBaseMap);

		String tmpPolicy = generatePolicyFile(replaceMap);
		
		File file = new File(tmpPolicy);
		String policyPath = StormConfig.worker_root(conf, workerId) + File.separator + SANBOX_TEMPLATE_NAME;
		File dest = new File(policyPath);
		file.renameTo(dest);
		
		StringBuilder sb = new StringBuilder();
		sb.append(" -Djava.security.manager -Djava.security.policy=");
		sb.append(policyPath);
		
		return sb.toString();
		
	}

	public static void main(String[] args) {
		Map<Object, Object> conf = Utils.readStormConfig();
		
		conf.put("java.sandbox.enable", Boolean.valueOf(true));
		
		SandBoxMaker maker = new SandBoxMaker(conf);
		
		try {
			System.out.println("sandboxPolicy:" + maker.sandboxPolicy("simple", new HashMap<String, String>()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
