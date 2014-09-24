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
package backtype.storm.messaging.netty;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);

    private ReentrantLock clientBufferSizeLock;
    private Condition clientBufferSizeCondition;
    private Map stormConf;
    private long clientBufferSize;
    private long clientBufferMessageSize;

    public ClientFactory(Map stormConf) {
        this.stormConf = stormConf;

        clientBufferSizeLock = new ReentrantLock();
        clientBufferSizeCondition = clientBufferSizeLock.newCondition();

        clientBufferSize = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_CLIENT_BUFFER_SIZE), 104857600); // default 100M
        clientBufferMessageSize = 0;
    }

    public void onMessageAdd(long taskMessageSize) {
        clientBufferSizeLock.lock();
        try {
            while (true) {
                if (clientBufferMessageSize > clientBufferSize) {
                    LOG.info("curBufferSize is:" + clientBufferMessageSize + ", we should wait");
                    clientBufferSizeCondition.await();
                } else {
                    break;
                }
            }

            clientBufferMessageSize += taskMessageSize;
        } catch (InterruptedException e) {
            LOG.warn("condition wait catch Exception: ", e);
        } finally {
            clientBufferSizeLock.unlock();
        }
    }

    public void onMessageSendFinished(long taskMessageSize) {
        clientBufferSizeLock.lock();
        clientBufferMessageSize -= taskMessageSize;
        assert (clientBufferMessageSize >= 0);
        clientBufferSizeCondition.signal();
        clientBufferSizeLock.unlock();
    }

}
