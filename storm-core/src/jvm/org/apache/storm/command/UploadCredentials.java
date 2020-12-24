/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.command;

import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadCredentials {

    private static final Logger LOG = LoggerFactory.getLogger(UploadCredentials.class);

    /**
     * Uploads credentials for a topology.
     * @param args To accept topology name.
     * @throws Exception on errors.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "file", null)
                                    .opt("u", "user", null)
                                    .boolOpt("e", "exception-when-empty")
                                    .arg("topologyName", CLI.FIRST_WINS)
                                    .optionalArg("rawCredentials", CLI.INTO_LIST)
                                    .parse(args);

        String credentialFile = (String) cl.get("f");
        List<String> rawCredentials = (List<String>) cl.get("rawCredentials");
        String topologyName = (String) cl.get("topologyName");
        Utils.validateTopologyName(topologyName);

        if (null != rawCredentials && ((rawCredentials.size() % 2) != 0)) {
            throw new RuntimeException("Need an even number of arguments to make a map");
        }
        Map<String, String> credentialsMap = new HashMap<>();
        if (null != credentialFile) {
            Properties credentialProps = new Properties();
            credentialProps.load(new FileReader(credentialFile));
            for (Map.Entry<Object, Object> credentialProp : credentialProps.entrySet()) {
                credentialsMap.put((String) credentialProp.getKey(),
                                   (String) credentialProp.getValue());
            }
        }
        if (null != rawCredentials) {
            for (int i = 0; i < rawCredentials.size(); i += 2) {
                credentialsMap.put(rawCredentials.get(i), rawCredentials.get(i + 1));
            }
        }

        Map<String, Object> topologyConf = new HashMap<>();
        //Try to get the topology conf from nimbus, so we can reuse it.
        try (NimbusClient nc = NimbusClient.getConfiguredClient(new HashMap<>())) {
            Nimbus.Iface client = nc.getClient();
            TopologySummary topo = client.getTopologySummaryByName(topologyName);
            //We found the topology, lets get the conf
            String topologyId = topo.get_id();
            topologyConf = (Map<String, Object>) JSONValue.parse(client.getTopologyConf(topologyId));
            LOG.info("Using topology conf from {} as basis for getting new creds", topologyId);

            Map<String, Object> commandLine = Utils.readCommandLineOpts();
            List<String> clCreds = (List<String>) commandLine.get(Config.TOPOLOGY_AUTO_CREDENTIALS);
            List<String> topoCreds = (List<String>) topologyConf.get(Config.TOPOLOGY_AUTO_CREDENTIALS);

            if (clCreds != null) {
                Set<String> extra = new HashSet<>(clCreds);
                if (topoCreds != null) {
                    extra.removeAll(topoCreds);
                }
                if (!extra.isEmpty()) {
                    LOG.warn("The topology {} is not using {} but they were included here.", topologyId, extra);
                }

                //Now check for autoCreds that are missing from the command line, but only if the
                // command line is used.
                if (topoCreds != null) {
                    Set<String> missing = new HashSet<>(topoCreds);
                    missing.removeAll(clCreds);
                    if (!missing.isEmpty()) {
                        LOG.warn("The topology {} is using {} but they were not included here.", topologyId, missing);
                    }
                }
            }
        }

        // use the local setting for the login config rather than the topology's
        topologyConf.remove("java.security.auth.login.config");

        boolean throwExceptionForEmptyCreds = (boolean) cl.get("e");
        boolean hasCreds = StormSubmitter.pushCredentials(topologyName, topologyConf, credentialsMap, (String) cl.get("u"));
        if (!hasCreds && throwExceptionForEmptyCreds) {
            String message = "No credentials were uploaded for " + topologyName;
            LOG.error(message);
            throw new RuntimeException(message);
        }
        LOG.info("Uploaded new creds to topology: {}", topologyName);
    }
}
