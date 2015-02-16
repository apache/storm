package com.alibaba.jstorm.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Network utilis
 * 
 * @author yannian
 * 
 */
public class NetWorkUtils {
	private static Logger LOG = Logger.getLogger(NetWorkUtils.class);

	public static String hostname() {
		String hostname = null;
		try {
			hostname = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOG.error("local_hostname", e);
		}
		return hostname;
	}
	
	public static String ip() {
		String hostname = null;
		try {
			hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			LOG.error("local_hostname", e);
		}
		return hostname;
	}

	/**
	 * Check whether the port is available to binding
	 * 
	 * @param port
	 * @return -1 means not available, others means available
	 * @throws IOException
	 */
	public static int tryPort(int port) throws IOException {
		ServerSocket socket = new ServerSocket(port);
		int rtn = socket.getLocalPort();
		socket.close();
		return rtn;
	}

	/**
	 * get one available port
	 * 
	 * @return -1 means failed, others means one availablePort
	 */
	public static int getAvailablePort() {
		return availablePort(0);
	}

	/**
	 * Check whether the port is available to binding
	 * 
	 * @param prefered
	 * @return -1 means not available, others means available
	 */
	public static int availablePort(int prefered) {
		int rtn = -1;
		try {
			rtn = tryPort(prefered);
		} catch (IOException e) {

		}
		return rtn;
	}
	
	public static String host2Ip(String host) {
		InetAddress address = null;
		try {
			address = InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			LOG.warn("NetWorkUtil can't transfer hostname(" + host + ") to ip, return hostname", e);
			return host;
		}
		return address.getHostAddress();
	}
	
	public static String ip2Host(String ip) {
		InetAddress address = null;
		try {
			address = InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			LOG.warn("NetWorkUtil can't transfer ip(" + ip + ") to hostname, return ip", e);
			return ip;
		}
		return address.getHostName();
	}
	
	public static boolean equals(String host1, String host2) {
		
		
		if (StringUtils.equalsIgnoreCase(host1, host2) == true) {
			return true;
		}
		
		if (host1 == null || host2 == null) {
			return false;
		}
		
		String ip1 = host2Ip(host1);
		String ip2 = host2Ip(host2);
		
		return StringUtils.equalsIgnoreCase(ip1, ip2);
		
	}
	
}
