/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network utilis
 * 
 * @author yannian
 * 
 */
public class NetWorkUtils {
    private static Logger LOG = LoggerFactory.getLogger(NetWorkUtils.class);

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
            LOG.warn("NetWorkUtil can't transfer hostname(" + host
                    + ") to ip, return hostname", e);
            return host;
        }
        return address.getHostAddress();
    }
    
    public static List<String>  host2Ip(List<String> servers) {
    	if (servers == null || servers.size() == 0) {
    		return new ArrayList<String>();
    	}
    	
    	Set<String> ret = new HashSet<String>();
    	for (String server : servers) {
    		if (StringUtils.isBlank(server)) {
    			continue;
    		}
    		
    		InetAddress ia;
			try {
				ia = InetAddress.getByName(server);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				LOG.info("Fail to get address of ", server);
				continue;
			}
    		if (ia.isLoopbackAddress() || ia.isAnyLocalAddress()) {
    			ret.add(NetWorkUtils.ip());
    		}else {
    			ret.add(ia.getHostAddress());
    		}
    	}
    	
    	
    	return JStormUtils.mk_list(ret);
    }

    public static String ip2Host(String ip) {
        InetAddress address = null;
        try {
            address = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            LOG.warn("NetWorkUtil can't transfer ip(" + ip
                    + ") to hostname, return ip", e);
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
    
    public static void main(String[] args) {
    	List<String> servers = new ArrayList<String>();
    	servers.add("localhost");
    	
    	System.out.println(host2Ip(servers));
    	
    }

}
