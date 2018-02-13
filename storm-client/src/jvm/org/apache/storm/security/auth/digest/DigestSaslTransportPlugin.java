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

package org.apache.storm.security.auth.digest;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.sasl.SaslTransportPlugin;
import org.apache.storm.security.auth.sasl.SimpleSaslClientCallbackHandler;
import org.apache.storm.security.auth.sasl.SimpleSaslServerCallbackHandler;
import org.apache.storm.security.auth.workertoken.WorkerTokenAuthorizer;
import org.apache.storm.security.auth.workertoken.WorkerTokenClientCallbackHandler;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DigestSaslTransportPlugin extends SaslTransportPlugin {
    public static final String DIGEST = "DIGEST-MD5";
    private static final Logger LOG = LoggerFactory.getLogger(DigestSaslTransportPlugin.class);

    protected TTransportFactory getServerTransportFactory() throws IOException {
        //create an authentication callback handler
        CallbackHandler serverCallbackHandler = new SimpleSaslServerCallbackHandler(
            new WorkerTokenAuthorizer(conf, type),
            new JassPasswordProvider(loginConf));

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(DIGEST, AuthUtils.SERVICE, "localhost", null, serverCallbackHandler);

        LOG.info("SASL DIGEST-MD5 transport factory will be used");
        return factory;
    }

    @Override
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws TTransportException, IOException {
        CallbackHandler clientCallbackHandler;
        WorkerToken token = WorkerTokenClientCallbackHandler.findWorkerTokenInSubject(type);
        if (token != null) {
            clientCallbackHandler = new WorkerTokenClientCallbackHandler(token);
        } else if (loginConf != null) {
            AppConfigurationEntry [] configurationEntries = loginConf.getAppConfigurationEntry(AuthUtils.LOGIN_CONTEXT_CLIENT);
            if (configurationEntries == null) {
                String errorMessage = "Could not find a '" + AuthUtils.LOGIN_CONTEXT_CLIENT
                    + "' entry in this configuration: Client cannot start.";
                throw new IOException(errorMessage);
            }

            String username = "";
            String password = "";
            for (AppConfigurationEntry entry : configurationEntries) {
                Map options = entry.getOptions();
                username = (String)options.getOrDefault("username", username);
                password = (String)options.getOrDefault("password", password);
            }
            clientCallbackHandler = new SimpleSaslClientCallbackHandler(username, password);
        } else {
            throw new IOException("Could not find any way to authenticate with the server.");
        }

        TSaslClientTransport wrapperTransport = new TSaslClientTransport(DIGEST,
                null,
                AuthUtils.SERVICE, 
                serverHost,
                null,
                clientCallbackHandler,
                transport);

        wrapperTransport.open();
        LOG.debug("SASL DIGEST-MD5 client transport has been established");

        return wrapperTransport;
    }

    @Override
    public boolean areWorkerTokensSupported() {
        return true;
    }
}
