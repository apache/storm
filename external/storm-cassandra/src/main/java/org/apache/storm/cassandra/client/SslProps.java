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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.client;

/**
 * Properties needed for enabling SSL connection to Cassandra.<br/>
 * 
 * @author c.friaszapater
 *
 */
public class SslProps {
    // eg: "SSL"
    private String securityProtocol;
    private String truststorePath;
    private String truststorePassword;
    private String keystorePath;
    private String keystorePassword;

    public static final String PROTOCOL_SSL = "SSL";

    /**
     * New SSL properties.
     */
    public SslProps(String securityProtocol, String truststorePath, String truststorePassword, String keystorePath,
            String keystorePassword) {
        if (protocolIsSslEnabled(securityProtocol)
                && (truststorePath == null || truststorePassword == null || keystorePath == null || keystorePassword == null)) {
            throw new IllegalStateException(
                    String.format("All SSL properties must be non null if protocol is SSL enabled, protocol was %s", securityProtocol));
        }

        this.securityProtocol = securityProtocol;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    /**
     * true if securityProtocol = SSL, false otherwise.
     */
    public boolean isSsl() {
        return protocolIsSslEnabled(securityProtocol);
    }

    private boolean protocolIsSslEnabled(String securityProtocol) {
        return securityProtocol != null && (PROTOCOL_SSL.equals(securityProtocol));
    }

    @Override
    public String toString() {
        return "SSLProps [securityProtocol=" + securityProtocol + ", truststorePath=" + truststorePath + ", truststorePassword="
                + truststorePassword + ", keystorePath=" + keystorePath + ", keystorePassword=" + keystorePassword + "]";
    }

}
