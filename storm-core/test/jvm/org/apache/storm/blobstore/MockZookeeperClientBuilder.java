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
package org.apache.storm.blobstore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.Pathable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class MockZookeeperClientBuilder {

    private static final Logger LOG = Logger.getLogger(MockZookeeperClientBuilder.class);

    private CuratorFramework zkClient = mock(CuratorFramework.class);

    private ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
    private GetChildrenBuilder getChildrenBuilder = mock(GetChildrenBuilder.class);

    public MockZookeeperClientBuilder() {
        when(zkClient.checkExists()).thenReturn(existsBuilder);
        when(zkClient.getChildren()).thenReturn(getChildrenBuilder);
    }

    public CuratorFramework build() {
        return zkClient;
    }

    private <T extends Pathable<U>, U> T mockForPath(T pathable, String path, U toReturn) {
        try {
            when(pathable.forPath(path)).thenReturn(toReturn);
        } catch (Exception e) {
            LOG.warn(e.toString());
        }
        return pathable;
    }

    public ExistsBuilder withExists(String path, boolean returnValue) {
        when(existsBuilder.watched()).thenReturn(existsBuilder);
        Stat stat = returnValue ? new Stat() : null;
        return mockForPath(existsBuilder, path, stat);
    }

    public GetChildrenBuilder withGetChildren(String path, String... returnValue) {
        return withGetChildren(path, Arrays.asList(returnValue));
    }

    public GetChildrenBuilder withGetChildren(String path, List<String> returnValue) {
        when(getChildrenBuilder.watched()).thenReturn(getChildrenBuilder);
        return mockForPath(getChildrenBuilder, path, returnValue);
    }

    public void verifyExists() {
        verifyExists(true);
    }

    public void verifyExists(boolean happened) {
        verifyExists(happened ? 1 : 0);
    }

    public void verifyExists(int times) {
        verify(zkClient, times(times)).checkExists();
    }

    public void verifyGetChildren() {
        verifyGetChildren(true);
    }

    public void verifyGetChildren(boolean happened) {
        verifyGetChildren(happened ? 1 : 0);
    }

    public void verifyGetChildren(int times) {
        verify(zkClient, times(times)).getChildren();
    }
}
