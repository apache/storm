package com.alipay.dw.jstorm.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

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
}
