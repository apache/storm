/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.pmml;

import com.google.common.collect.Lists;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.model.jpmml.JpmmlModelOutputs;
import org.apache.storm.pmml.runner.jpmml.JpmmlFactory;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

/**
 * Topology that loads a PMML Model and raw input data from a CSV file. The {@link RawInputFromCSVSpout}
 * creates a stream of tuples with raw inputs, and the {@link PMMLPredictorBolt} computes the predicted scores.
 *
 * The location of the PMML Model and CSV files can be specified as CLI argument. If no arguments are given,
 * it loads the default example as described in the README file
 */
public class JpmmlRunnerTestTopology {
    private static final String RAW_INPUT_FROM_CSV_SPOUT = "rawInputFromCsvSpout";
    private static final String PMML_PREDICTOR_BOLT = "pmmLPredictorBolt";
    private static final String PRINT_BOLT = "printBolt";

    private File rawInputs;           // Raw input data to be scored (predicted)
    private File pmml;                // PMML Model
    private boolean isLocal;
    private String tplgyName;

    public static void main(String[] args) throws Exception {
        try {
            JpmmlRunnerTestTopology testTopology = new JpmmlRunnerTestTopology();
            testTopology.parseArgs(args);
            System.out.println(String.format("Running topology using PMML model loaded from [%s] and raw input data loaded from [%s]",
                    testTopology.pmml.getAbsolutePath(), testTopology.rawInputs.getAbsolutePath()));
            testTopology.run();
        } catch (Exception e) {
            printUsage();
            throw new RuntimeException(e);
        }
    }

    private void parseArgs(String[] args) {
        if (Arrays.stream(args).anyMatch(option -> option.equals("-h"))) {
            printUsage();
        } else {
            if (args.length < 3) {
                tplgyName = "pmmlPredictorLocal";
                isLocal = true;
                if (args.length == 0) {     // run local examples
                    pmml = loadExample(pmml, "KNIME_PMML_4.1_Examples_single_audit_logreg.xml");
                    rawInputs = loadExample(rawInputs, "Audit.50.csv");
                } else {
                    pmml = new File(args[0]);
                    rawInputs = new File(args[1]);
                }
            } else {
                tplgyName = args[0];
                pmml = new File(args[1]);
                rawInputs = new File(args[2]);
                isLocal = false;
            }
        }
    }

    private File loadExample(File file, String example) {
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(example)) {
            file = File.createTempFile("pmml-example", ".tmp");
            IOUtils.copy(stream, new FileOutputStream(file));
        } catch (IOException e) {
            throw new RuntimeException("Error loading example " + example, e);
        }
        return file;
    }

    private static void printUsage() {
        System.out.println("Usage: java [" + JpmmlRunnerTestTopology.class.getName()
                + " topology_name] [<PMML model file path> <Raw inputs CSV file path>]");
    }

    private void run() throws Exception {
        if (isLocal) {
            submitTopologyLocalCluster(newTopology(), newConfig());
        } else {
            submitTopologyRemoteCluster(newTopology(), newConfig());
        }
    }

    private StormTopology newTopology() throws Exception {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(RAW_INPUT_FROM_CSV_SPOUT, RawInputFromCSVSpout.newInstance(rawInputs));
        builder.setBolt(PMML_PREDICTOR_BOLT, newBolt()).shuffleGrouping(RAW_INPUT_FROM_CSV_SPOUT);
        builder.setBolt(PRINT_BOLT, new PrinterBolt()).shuffleGrouping(PMML_PREDICTOR_BOLT);
        return builder.createTopology();
    }

    private void submitTopologyLocalCluster(StormTopology topology, Config config) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(tplgyName, config, topology);
        stopWaitingForInput();
    }

    private void submitTopologyRemoteCluster(StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(tplgyName, config, topology);
    }

    private void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Config newConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private IRichBolt newBolt() throws Exception {
        final List<String> streams = Lists.newArrayList(Utils.DEFAULT_STREAM_ID, "NON_DEFAULT_STREAM_ID");
        final ModelOutputs outFields = JpmmlModelOutputs.toStreams(pmml, streams);
        return new PMMLPredictorBolt(new JpmmlFactory.ModelRunnerCreator(pmml, outFields), outFields);
    }

    private static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }
    }
}
