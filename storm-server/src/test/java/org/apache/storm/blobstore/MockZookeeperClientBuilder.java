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

package org.apache.storm.blobstore;

import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.zookeeper.ZkClient;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MockZookeeperClientBuilder {

    private static final Logger LOG = Logger.getLogger(MockZookeeperClientBuilder.class);

    private final ZkClient zkClient = mock(ZkClient.class);

    public ZkClient build() {
        return zkClient;
    }

    public void withExists(String path, boolean returnValue) {
        Stat stat = returnValue ? new Stat() : null;
        try {
            when(zkClient.exists(eq(path), anyBoolean())).thenReturn(stat);
        } catch (Exception e) {
            LOG.warn(e.toString());
        }
    }

    public void withGetChildren(String path, String... returnValue) {
        withGetChildren(path, Arrays.asList(returnValue));
    }

    public void withGetChildren(String path, List<String> returnValue) {
        try {
            when(zkClient.getChildren(eq(path), anyBoolean())).thenReturn(returnValue);
        } catch (Exception e) {
            LOG.warn(e.toString());
        }
    }

    public void verifyExists() {
        verifyExists(true);
    }

    public void verifyExists(boolean happened) {
        verifyExists(happened ? 1 : 0);
    }

    public void verifyExists(int times) {
        try {
            verify(zkClient, times(times)).exists(anyString(), anyBoolean());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void verifyGetChildren() {
        verifyGetChildren(true);
    }

    public void verifyGetChildren(boolean happened) {
        verifyGetChildren(happened ? 1 : 0);
    }

    public void verifyGetChildren(int times) {
        try {
            verify(zkClient, times(times)).getChildren(anyString(), anyBoolean());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
