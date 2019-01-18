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

package org.apache.storm.cluster;

import org.apache.storm.assignments.LocalAssignmentsBackendFactory;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClusterStateImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(StormClusterStateImplTest.class);
    private final String[] pathlist = {
        ClusterUtils.ASSIGNMENTS_SUBTREE,
        ClusterUtils.STORMS_SUBTREE,
        ClusterUtils.SUPERVISORS_SUBTREE,
        ClusterUtils.WORKERBEATS_SUBTREE,
        ClusterUtils.ERRORS_SUBTREE,
        ClusterUtils.BLOBSTORE_SUBTREE,
        ClusterUtils.NIMBUSES_SUBTREE,
        ClusterUtils.LOGCONFIG_SUBTREE
    };

    private IStateStorage storage;
    private ClusterStateContext context;
    private StormClusterStateImpl state;

    @Before
    public void init() throws Exception {
        storage = Mockito.mock(IStateStorage.class);
        context = new ClusterStateContext();
        state = new StormClusterStateImpl(storage, LocalAssignmentsBackendFactory.getDefault(), context, false /*solo*/);
    }


    @Test
    public void registeredCallback() {
        Mockito.verify(storage).register(Matchers.<ZKStateChangedCallback>anyObject());
    }

    @Test
    public void createdZNodes() {
        for (String path : pathlist) {
            Mockito.verify(storage).mkdirs(path, null);
        }
    }
}

