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

package org.apache.storm.starter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreAPIWordCountTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreAPIWordCountTopology.class);
    private static ClientBlobStore store; // Client API to invoke blob store API functionality
    private static String key = "key";
    private static String fileName = "blacklist.txt";

    public static void prepare() {
        Config conf = new Config();
        conf.putAll(Utils.readStormConfig());
        store = Utils.getClientBlobStore(conf);
    }

    // Equivalent create command on command line
    // storm blobstore create --file blacklist.txt --acl o::rwa key
    private static void createBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, Path file)
        throws AuthorizationException, KeyAlreadyExistsException, IOException, KeyNotFoundException {
        String stringBlobACL = "o::rwa";
        AccessControl blobACL = BlobStoreAclHandler.parseAccessControl(stringBlobACL);
        List<AccessControl> acls = new LinkedList<AccessControl>();
        acls.add(blobACL); // more ACLs can be added here
        SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
        AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey, settableBlobMeta);
        blobStream.write(readFile(file).getBytes(Charset.defaultCharset()));
        blobStream.close();
    }

    // Equivalent update command on command line
    // storm blobstore update --file blacklist.txt key
    private static void updateBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, Path file)
        throws KeyNotFoundException, AuthorizationException, IOException {
        AtomicOutputStream blobOutputStream = clientBlobStore.updateBlob(blobKey);
        blobOutputStream.write(readFile(file).getBytes(Charset.defaultCharset()));
        blobOutputStream.close();
    }

    private static String getRandomSentence() {
        String[] sentences = new String[]{
            "the cow jumped over the moon", "an apple a day keeps the doctor away",
            "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"
        };
        String sentence = sentences[new Random().nextInt(sentences.length)];
        return sentence;
    }

    private static Set<String> getRandomWordSet() {
        Set<String> randomWordSet = new HashSet<>();
        Random random = new Random();
        String[] words = new String[]{
            "cow", "jumped", "over", "the", "moon", "apple", "day", "doctor", "away",
            "four", "seven", "ago", "snow", "white", "seven", "dwarfs", "nature", "two"
        };
        // Choosing atmost 5 words to update the blacklist file for filtering
        for (int i = 0; i < 5; i++) {
            randomWordSet.add(words[random.nextInt(words.length)]);
        }
        return randomWordSet;
    }

    private static Set<String> parseFile(String fileName) throws IOException {
        Path file = Paths.get(fileName);
        Set<String> wordSet = new HashSet<>();
        if (!file.toFile().exists()) {
            return wordSet;
        }
        StringTokenizer tokens = new StringTokenizer(readFile(file), "\r\n");
        while (tokens.hasMoreElements()) {
            wordSet.add(tokens.nextToken());
        }
        LOG.debug("parseFile {}", wordSet);
        return wordSet;
    }

    private static String readFile(Path file) throws IOException {
        // Do not use canonical file name here as we are using
        // symbolic links to read file data and performing atomic move
        // while updating files
        return Files.readAllLines(file, Charset.defaultCharset()).stream()
            .collect(Collectors.joining(System.lineSeparator()));
    }

    // Creating a blacklist file to read from the disk
    public static Path createFile(String fileName) throws IOException {
        Path file = Paths.get(fileName);
        if (!file.toFile().exists()) {
            Files.createFile(file);
        }
        writeToFile(file, getRandomWordSet());
        return file;
    }

    // Updating a blacklist file periodically with random words
    public static Path updateFile(Path file) throws IOException {
        writeToFile(file, getRandomWordSet());
        return file;
    }

    // Writing random words to be blacklisted
    public static void writeToFile(Path file, Set<String> content) throws IOException {
        Files.write(file, content, Charset.defaultCharset());
    }

    public static void main(String[] args) {
        prepare();
        BlobStoreAPIWordCountTopology wc = new BlobStoreAPIWordCountTopology();
        try {
            Path file = createFile(fileName);
            // Creating blob again before launching topology
            createBlobWithContent(key, store, file);

            // Blostore launch command with topology blobstore map
            // Here we are giving it a local name so that we can read from the file
            // bin/storm jar examples/storm-starter/storm-starter-topologies-0.11.0-SNAPSHOT.jar
            // org.apache.storm.starter.BlobStoreAPIWordCountTopology bl -c
            // topology.blobstore.map='{"key":{"localname":"blacklist.txt", "uncompress":"false"}}'
            wc.buildAndLaunchWordCountTopology(args);

            // Updating file few times every 5 seconds
            for (int i = 0; i < 10; i++) {
                updateBlobWithContent(key, store, updateFile(file));
                Utils.sleep(5000);
            }
        } catch (KeyAlreadyExistsException kae) {
            LOG.info("Key already exists {}", kae);
        } catch (AuthorizationException | KeyNotFoundException | IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    public void buildAndLaunchWordCountTopology(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 5);
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("filter", new FilterWords(), 6).shuffleGrouping("split");

        Config conf = new Config();
        conf.setDebug(true);
        try {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } catch (InvalidTopologyException | AuthorizationException | AlreadyAliveException exp) {
            throw new RuntimeException(exp);
        }
    }

    // Spout implementation
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            _collector.emit(new Values(getRandomSentence()));
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

    }

    // Bolt implementation
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class FilterWords extends BaseBasicBolt {
        boolean poll = false;
        long pollTime;
        Set<String> wordSet;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            // Thread Polling every 5 seconds to update the wordSet seconds which is
            // used in FilterWords bolt to filter the words
            try {
                if (!poll) {
                    wordSet = parseFile(fileName);
                    pollTime = System.currentTimeMillis();
                    poll = true;
                } else {
                    if ((System.currentTimeMillis() - pollTime) > 5000) {
                        wordSet = parseFile(fileName);
                        pollTime = System.currentTimeMillis();
                    }
                }
            } catch (IOException exp) {
                throw new RuntimeException(exp);
            }
            if (wordSet != null && !wordSet.contains(word)) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
}


