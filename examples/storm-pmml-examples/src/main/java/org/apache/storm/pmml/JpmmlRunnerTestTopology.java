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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.storm.Config;
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

/**
 * Topology that loads a PMML Model and raw input data from a CSV file. The {@link RawInputFromCSVSpout}
 * creates a stream of tuples with raw inputs, and the {@link PMMLPredictorBolt} computes the predicted scores.
 *
 * <p>The location of the PMML Model and CSV files can be specified as CLI argument. Alternatively, the PMML Model can also
 * be uploaded to the Blobstore and used in the topology specifying the blobKey. If no arguments are given,
 * it loads the default example as described in the README file
 */
public class JpmmlRunnerTestTopology {
    private static final String PMML_MODEL_FILE = "KNIME_PMML_4.1_Examples_single_audit_logreg.xml";
    private static final String RAW_INPUTS_FILE = "Audit.50.csv";

    private static final String RAW_INPUT_FROM_CSV_SPOUT = "rawInputFromCsvSpout";
    private static final String PMML_PREDICTOR_BOLT = "pmmLPredictorBolt";
    private static final String PRINT_BOLT_1 = "printBolt1";
    private static final String PRINT_BOLT_2 = "printBolt2";
    private static final String NON_DEFAULT_STREAM_ID = "NON_DEFAULT_STREAM_ID";

    private File rawInputs;           // Raw input data to be scored (predicted)
    private File pmml;                // PMML Model read from file - null if using Blobstore
    private String blobKey;           // PMML Model downloaded from Blobstore - null if using File
    private String tplgyName = "test";

    public static void main(String[] args) throws Exception {
        try {
            JpmmlRunnerTestTopology testTopology = new JpmmlRunnerTestTopology();
            testTopology.parseArgs(args);
            testTopology.run();
        } catch (Exception e) {
            e.printStackTrace();
            printUsage();
        }
    }

    private void parseArgs(String[] args) {
        if (Arrays.stream(args).anyMatch(option -> option.equals("-h"))) {
            printUsage();
        } else if (Arrays.stream(args).anyMatch(option -> option.equals("-f"))
                && Arrays.stream(args).anyMatch(option -> option.equals("-b"))) {
            System.out.println("Please specify only one option of [-b, -f]");
            printUsage();
        } else {
            try {
                for (int i = 0; i < args.length; ) {
                    switch (args[i]) {
                        case "-f":
                            pmml = new File(args[i + 1]);
                            i += 2;
                            break;
                        case "-b":
                            blobKey = args[i + 1];
                            i += 2;
                            break;
                        case "-r":
                            rawInputs = new File(args[i + 1]);
                            i += 2;
                            break;
                        default:
                            tplgyName = args[i];
                            i++;
                            break;
                    }
                }
                setDefaults();
            } catch (Exception e) {
                e.printStackTrace();
                printUsage();
            }
        }
    }

    private void setDefaults() {
        if (blobKey == null) {  // blob key not specified, use file
            if (pmml == null) {
                pmml = loadExample(pmml, PMML_MODEL_FILE);
            }
        }

        if (rawInputs == null) {
            rawInputs = loadExample(rawInputs, RAW_INPUTS_FILE);
        }

        if (tplgyName == null) {
            tplgyName = "pmmlPredictorLocal";
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
        System.out.println("Usage: " + JpmmlRunnerTestTopology.class.getName()
                + " [[[-f <PMML model file path>] [-b <Blobstore key used to upload PMML Model>]] "
                + "-r <Raw inputs CSV file path>] [topology_name]");
        System.exit(1);
    }

    private void run() throws Exception {
        System.out.println(String.format("Running topology using PMML model loaded from [%s] and raw input data loaded from [%s]",
                blobKey != null ? "Blobstore with blob key [" + blobKey + "]" : pmml.getAbsolutePath(), rawInputs.getAbsolutePath()));
        submitTopologyRemoteCluster(newTopology(), newConfig());
    }

    private StormTopology newTopology() throws Exception {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(RAW_INPUT_FROM_CSV_SPOUT, RawInputFromCSVSpout.newInstance(rawInputs));
        builder.setBolt(PMML_PREDICTOR_BOLT, newBolt()).shuffleGrouping(RAW_INPUT_FROM_CSV_SPOUT);
        builder.setBolt(PRINT_BOLT_1, new PrinterBolt()).shuffleGrouping(PMML_PREDICTOR_BOLT);
        builder.setBolt(PRINT_BOLT_2, new PrinterBolt()).shuffleGrouping(PMML_PREDICTOR_BOLT, NON_DEFAULT_STREAM_ID);
        return builder.createTopology();
    }

    private void submitTopologyRemoteCluster(StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(tplgyName, config, topology);
    }

    private Config newConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private IRichBolt newBolt() throws Exception {
        final List<String> streams = Lists.newArrayList(Utils.DEFAULT_STREAM_ID, NON_DEFAULT_STREAM_ID);
        if (blobKey != null) {  // Load PMML Model from Blob store
            final ModelOutputs outFields = JpmmlModelOutputs.toStreams(blobKey, streams);
            return new PMMLPredictorBolt(new JpmmlFactory.ModelRunnerFromBlobStore(blobKey, outFields), outFields);
        } else {                // Load PMML Model from File
            final ModelOutputs outFields = JpmmlModelOutputs.toStreams(pmml, streams);
            return new PMMLPredictorBolt(new JpmmlFactory.ModelRunnerFromFile(pmml, outFields), outFields);
        }
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
