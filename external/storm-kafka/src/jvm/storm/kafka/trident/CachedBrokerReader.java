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
package storm.kafka.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedBrokerReader implements IBrokerReader {

    public static final Logger LOG = LoggerFactory.getLogger(CachedBrokerReader.class);

    private GlobalPartitionInformation cachedBrokers;
    private IBrokerReader reader;
    private long lastRefreshTimeMs;
    private long refreshMillis;

    public CachedBrokerReader(IBrokerReader reader, long refreshFreqSecs) {
        this.reader = reader;
        cachedBrokers = reader.getCurrentBrokers();
        lastRefreshTimeMs = System.currentTimeMillis();
        refreshMillis = refreshFreqSecs * 1000L;
    }

    @Override
    public GlobalPartitionInformation getCurrentBrokers() {
        long currTime = System.currentTimeMillis();
        if (currTime > lastRefreshTimeMs + refreshMillis) {
            LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
            cachedBrokers = reader.getCurrentBrokers();
            lastRefreshTimeMs = currTime;
        }
        return cachedBrokers;
    }

    @Override
    public void close() {
        reader.close();
    }
}
