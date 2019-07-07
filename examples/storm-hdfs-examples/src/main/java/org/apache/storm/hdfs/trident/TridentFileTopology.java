/*
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

package org.apache.storm.hdfs.trident;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.yaml.snakeyaml.Yaml;

public class TridentFileTopology {

    public static StormTopology buildTopology(String hdfsUrl) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence", "key"), 1000, new Values("the cow jumped over the moon", 1L),
                                                    new Values("the man went to the store and bought some candy", 2L),
                                                    new Values("four score and seven years ago", 3L),
                                                    new Values("how many apples can you eat", 4L),
                                                    new Values("to be or not to be the person", 5L));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        Fields hdfsFields = new Fields("sentence", "key");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPath("/tmp/trident")
            .withPrefix("trident")
            .withExtension(".txt");

        RecordFormat recordFormat = new DelimitedRecordFormat()
            .withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(recordFormat)
            .withRotationPolicy(rotationPolicy)
            .withFsUrl(hdfsUrl)
            .withConfigKey("hdfs.config");

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        TridentState state = stream
            .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);

        Yaml yaml = new Yaml();
        InputStream in = new FileInputStream(args[1]);
        Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
        in.close();
        conf.put("hdfs.config", yamlConf);
        String topoName = "wordCounter";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3) {
            System.out.println("Usage: TridentFileTopology [hdfs url] [hdfs yaml config file] <topology name>");
            return;
        }
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology(topoName, conf, buildTopology(args[0]));
    }
}
