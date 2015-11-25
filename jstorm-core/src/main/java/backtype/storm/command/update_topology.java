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
package backtype.storm.command;

import backtype.storm.GenericOptionsParser;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiaojian.fxj on 2015/10/13.
 */
public class update_topology {
    public static final String UPDATE_CONF = "-conf";

    public static final String UPDATE_JAR = "-jar";

    public static void usage() {
        System.out.println("update topology config, please do as following:");
        System.out.println("update_topology topologyName -conf configFile");

        System.out.println("update topology jar, please do as following:");
        System.out.println("update_topology topologyName -jar jarFile");

        System.out.println("update topology jar and conf, please do as following:");
        System.out.println("update_topology topologyName -jar jarFile -conf configFile");
    }

    private static Options buildGeneralOptions(Options opts) {
        Options r = new Options();

        for (Object o : opts.getOptions())
            r.addOption((Option) o);

        Option jar = OptionBuilder.withArgName("path").hasArg()
                .withDescription("comma  jar of the submitted topology")
                .create("jar");
        r.addOption(jar);

        Option conf = OptionBuilder.withArgName("configuration file").hasArg()
                .withDescription("an application configuration file")
                .create("conf");
        r.addOption(conf);
        return r;
    }

    private static void updateTopology(String topologyName, String pathJar,
            String pathConf) {
        NimbusClient client = null;
        Map loadMap = null;
        if (pathConf != null) {
            loadMap = Utils.loadConf(pathConf);
        } else {
            loadMap = new HashMap();
        }

        Map conf = Utils.readStormConfig();

        conf.putAll(loadMap);
        client = NimbusClient.getConfiguredClient(conf);
        try {
            // update jar
            String uploadLocation = null;
            if (pathJar != null) {
                System.out.println("Jar update to master yet. Submitting jar of " + pathJar);
                String path = client.getClient().beginFileUpload();
                String[] pathCache = path.split("/");
                uploadLocation = path + "/stormjar-" + pathCache[pathCache.length - 1] + ".jar";
                List<String> lib = (List<String>) conf .get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
                Map<String, String> libPath = (Map<String, String>) conf .get(GenericOptionsParser.TOPOLOGY_LIB_PATH);
                if (lib != null && lib.size() != 0) {
                    for (String libName : lib) {
                        String jarPath = path + "/lib/" + libName;
                        client.getClient().beginLibUpload(jarPath);
                        StormSubmitter.submitJar(conf, libPath.get(libName), jarPath, client);
                    }

                } else {
                    if (pathJar == null) {
                        // no lib, no client jar
                        throw new RuntimeException( "No client app jar, please upload it");
                    }
                }

                if (pathJar != null) {
                    StormSubmitter.submitJar(conf, pathJar, uploadLocation, client);
                } else {
                    // no client jar, but with lib jar
                    client.getClient().finishFileUpload(uploadLocation);
                }
            }

            // update topology
            String jsonConf = Utils.to_json(loadMap);
            System.out.println("New configuration:\n" + jsonConf);

            client.getClient().updateTopology(topologyName, uploadLocation,
                    jsonConf);

            System.out.println("Successfully submit command update " + topologyName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            System.out.println("Invalid parameter");
            usage();
            return;
        }
        String topologyName = args[0];
        try {
            String[] str2 = Arrays.copyOfRange(args, 1, args.length);
            CommandLineParser parser = new GnuParser();
            Options r = buildGeneralOptions(new Options());
            CommandLine commandLine = parser.parse(r, str2, true);

            String pathConf = null;
            String pathJar = null;
            if (commandLine.hasOption("conf")) {
                pathConf = (commandLine.getOptionValues("conf"))[0];
            }
            if (commandLine.hasOption("jar")) {
                pathJar = (commandLine.getOptionValues("jar"))[0];
            }
            if (pathConf != null || pathJar != null)
                updateTopology(topologyName, pathJar, pathConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
