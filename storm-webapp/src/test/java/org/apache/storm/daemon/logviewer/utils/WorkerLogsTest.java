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

package org.apache.storm.daemon.logviewer.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Utils;
import org.junit.Test;

public class WorkerLogsTest {

    /**
     * Build up workerid-workerlogdir map for the old workers' dirs.
     */
    @Test
    public void testIdentifyWorkerLogDirs() throws Exception {
        try (TmpPath testDir = new TmpPath()) {
            Path port1Dir = Files.createDirectories(testDir.getFile().toPath().resolve("workers-artifacts/topo1/port1"));
            Path metaFile = Files.createFile(testDir.getFile().toPath().resolve("worker.yaml"));

            String expId = "id12345";
            SortedSet<Path> expected = new TreeSet<>();
            expected.add(port1Dir);
            SupervisorUtils mockedSupervisorUtils = mock(SupervisorUtils.class);
            SupervisorUtils.setInstance(mockedSupervisorUtils);

            Map<String, Object> stormConf = Utils.readStormConfig();
            WorkerLogs workerLogs = new WorkerLogs(stormConf, port1Dir, new StormMetricsRegistry()) {
                @Override
                public Optional<Path> getMetadataFileForWorkerLogDir(Path logDir) throws IOException {
                    return Optional.of(metaFile);
                }

                @Override
                public String getWorkerIdFromMetadataFile(Path metaFile) {
                    return expId;
                }
            };

            when(mockedSupervisorUtils.readWorkerHeartbeatsImpl(anyMap())).thenReturn(null);
            assertEquals(expected, workerLogs.getLogDirs(Collections.singleton(port1Dir), (wid) -> true));
        } finally {
            SupervisorUtils.resetInstance();
        }
    }

}
