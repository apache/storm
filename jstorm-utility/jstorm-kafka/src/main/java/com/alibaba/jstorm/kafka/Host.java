package com.alibaba.jstorm.kafka;

import java.io.Serializable;
/**
 * 
 * @author feilaoda
 *
 */
public class Host implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -315213440689707962L;
    private String host;
    private int port;

    public Host(String host) {
        this(host, 9092);
    }

    public Host(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (obj instanceof Host) {
            final Host other = (Host) obj;
            return this.host.equals(other.host) && this.port == other.port;
        } else {
            return false;
        }
    }
}
