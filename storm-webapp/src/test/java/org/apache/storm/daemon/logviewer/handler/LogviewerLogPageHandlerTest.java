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

package org.apache.storm.daemon.logviewer.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.Utils;
import org.assertj.core.util.Lists;
import org.junit.Test;

public class LogviewerLogPageHandlerTest {

    /**
     * list-log-files filter selects the correct log files to return.
     */
    @Test
    public void testListLogFiles() throws IOException {
        FileAttribute[] attrs = new FileAttribute[0];
        String rootPath = Files.createTempDirectory("workers-artifacts", attrs).toFile().getCanonicalPath();
        File file1 = new File(String.join(File.separator, rootPath, "topoA", "1111"), "worker.log");
        File file2 = new File(String.join(File.separator, rootPath, "topoA", "2222"), "worker.log");
        File file3 = new File(String.join(File.separator, rootPath, "topoB", "1111"), "worker.log");

        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        file3.createNewFile();

        String origin = "www.origin.server.net";
        Map<String, Object> stormConf = Utils.readStormConfig();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        LogviewerLogPageHandler handler = new LogviewerLogPageHandler(rootPath, null,
                new WorkerLogs(stormConf, new File(rootPath), metricsRegistry), new ResourceAuthorizer(stormConf), metricsRegistry);

        final Response expectedAll = LogviewerResponseBuilder.buildSuccessJsonResponse(
                Lists.newArrayList("topoA/port1/worker.log", "topoA/port2/worker.log", "topoB/port1/worker.log"),
                null,
                origin
        );

        final Response expectedFilterPort = LogviewerResponseBuilder.buildSuccessJsonResponse(
                Lists.newArrayList("topoA/port1/worker.log", "topoB/port1/worker.log"),
                null,
                origin
        );

        final Response expectedFilterTopoId = LogviewerResponseBuilder.buildSuccessJsonResponse(
                Lists.newArrayList("topoB/port1/worker.log"),
                null,
                origin
        );

        final Response returnedAll = handler.listLogFiles("user", null, null, null, origin);
        final Response returnedFilterPort = handler.listLogFiles("user", 1111, null, null, origin);
        final Response returnedFilterTopoId = handler.listLogFiles("user", null, "topoB", null, origin);

        Utils.forceDelete(rootPath);

        assertEqualsJsonResponse(expectedAll, returnedAll, List.class);
        assertEqualsJsonResponse(expectedFilterPort, returnedFilterPort, List.class);
        assertEqualsJsonResponse(expectedFilterTopoId, returnedFilterTopoId, List.class);
    }

    private <T> void assertEqualsJsonResponse(Response expected, Response actual, Class<T> entityClass) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        T entityFromExpected = objectMapper.readValue((String) expected.getEntity(), entityClass);
        T actualFromExpected = objectMapper.readValue((String) expected.getEntity(), entityClass);
        assertEquals(entityFromExpected, actualFromExpected);

        assertEquals(expected.getStatus(), actual.getStatus());
        assertTrue(expected.getHeaders().equalsIgnoreValueOrder(actual.getHeaders()));
    }
}
