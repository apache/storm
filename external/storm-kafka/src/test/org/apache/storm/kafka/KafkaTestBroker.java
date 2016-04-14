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
package org.apache.storm.kafka;

import org.apache.curator.test.InstanceSpec;

import kafka.server.KafkaConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Properties;
import kafka.server.KafkaServer;
import kafka.utils.Time;

/**
 * Date: 11/01/2014 Time: 13:15
 */
public class KafkaTestBroker {

    private int port;
    private KafkaServer kafka;
    private File logDir;

    public KafkaTestBroker(String zookeeperConnectionString) {
        this(zookeeperConnectionString, "0");
    }

    public KafkaTestBroker(String zookeeperConnectionString, String brokerId) {
        try {
            port = InstanceSpec.getRandomPort();
            logDir = new File(System.getProperty("java.io.tmpdir"), "kafka/logs/kafka-test-" + port);
            KafkaConfig config = buildKafkaConfig(zookeeperConnectionString, brokerId);
            //Java doesn't know how to call scala functions with default types, hence this hack.
            scala.Option<String> scalaNoneHack = scala.Option$.MODULE$.apply(null);
            kafka = new KafkaServer(config, new SystemTime(), scalaNoneHack);
            kafka.startup();
        } catch (Exception ex) {
            throw new RuntimeException("Could not start test broker", ex);
        }
    }

    private kafka.server.KafkaConfig buildKafkaConfig(String zookeeperConnectionString, String brokerId) {
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", zookeeperConnectionString);
        p.setProperty("broker.id", brokerId);
        p.setProperty("advertised.host.name", "localhost");
        p.setProperty("port", "" + port);
        p.setProperty("log.dirs", logDir.getAbsolutePath());
        return new KafkaConfig(p);
    }

    public String getBrokerConnectionString() {
        return "localhost:" + port;
    }

    public int getPort() {
        return port;
    }

    public void shutdown() {
        kafka.shutdown();
        kafka.awaitShutdown();
        FileUtils.deleteQuietly(logDir);
    }

    private static class SystemTime implements Time {

        public long milliseconds() {
            return System.currentTimeMillis();
        }

        public long nanoseconds() {
            return System.nanoTime();
        }

        public void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }
}
