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

package org.apache.storm.loadgen;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.loadgen.CaptureLoad;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Estimate the throughput of all topologies.
 */
public class EstimateThroughput {
    private static final Logger LOG = LoggerFactory.getLogger(EstimateThroughput.class);

    /**
     * Main entry point for estimate throughput command.
     * @param args the command line arguments.
     * @throws Exception on any error.
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Print a help message")
            .build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        boolean printHelp = false;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("ERROR " + e.getMessage());
            printHelp = true;
        }
        if (printHelp || cmd.hasOption('h')) {
            new HelpFormatter().printHelp("EstimateThroughput [options] [topologyName]*", options);
            return;
        }

        Config conf = new Config();
        int exitStatus = -1;

        List<TopologyLoadConf> regular = new ArrayList<>();
        List<TopologyLoadConf> trident = new ArrayList<>();

        try (NimbusClient nc = NimbusClient.getConfiguredClient(conf)) {
            Nimbus.Iface client = nc.getClient();
            List<String> topologyNames = cmd.getArgList();

            for (TopologySummary topologySummary: client.getTopologySummaries()) {
                if (topologyNames.isEmpty() || topologyNames.contains(topologySummary.get_name())) {
                    TopologyLoadConf capturedConf = CaptureLoad.captureTopology(client, topologySummary);
                    if (capturedConf.looksLikeTrident()) {
                        trident.add(capturedConf);
                    } else {
                        regular.add(capturedConf);
                    }
                }
            }

            System.out.println("TOPOLOGY\tTOTAL MESSAGES/sec\tESTIMATED INPUT MESSAGES/sec");
            for (TopologyLoadConf tl: regular) {
                System.out.println(tl.name + "\t" + tl.getAllEmittedAggregate() + "\t" + tl.getSpoutEmittedAggregate());
            }
            for (TopologyLoadConf tl: trident) {
                System.out.println(tl.name + "\t" + tl.getAllEmittedAggregate() + "\t" + tl.getTridentEstimatedEmittedAggregate());
            }
            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error trying to capture topologies...", e);
        } finally {
            System.exit(exitStatus);
        }
    }
}
