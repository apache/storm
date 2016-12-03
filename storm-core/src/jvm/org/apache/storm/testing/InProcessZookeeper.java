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

package org.apache.storm.testing;

import java.util.List;

import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;

/**
 * A local Zookeeper instance available for testing.
 * try (InProcessZookeeper zk = new InProcessZookeeper) {
 *   // Run Tests...
 * }
 */
public class InProcessZookeeper implements AutoCloseable {
    
    private final TmpPath zkTmp;
    private final NIOServerCnxnFactory zookeeper;
    private final long zkPort;
    
    public InProcessZookeeper() throws Exception {
        zkTmp = new TmpPath();
        @SuppressWarnings("unchecked")
        List<Object> portAndHandle = Zookeeper.mkInprocessZookeeper(zkTmp.getPath(), null);
        zkPort = (Long) portAndHandle.get(0);
        zookeeper = (NIOServerCnxnFactory) portAndHandle.get(1);
    }
    
    /**
     * @return the port ZK is listening on (localhost)
     */
    public long getPort() {
        return zkPort;
    }

    @Override
    public void close() throws Exception {
        Zookeeper.shutdownInprocessZookeeper(zookeeper);
        zkTmp.close();
    }
    
}