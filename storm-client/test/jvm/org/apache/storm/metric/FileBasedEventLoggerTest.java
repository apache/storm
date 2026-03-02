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

package org.apache.storm.metric;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileBasedEventLoggerTest {

    private Path tempDir;
    private FileBasedEventLogger eventLogger;

    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("storm-eventlogger-test");
        eventLogger = new FileBasedEventLogger();
    }

    @AfterEach
    public void tearDown() throws IOException {
        eventLogger.close();
        if (tempDir != null) {
            Files.walk(tempDir)
                    .map(Path::toFile)
                    .forEach(File::delete);
            tempDir.toFile().delete();
        }
    }

    private TopologyContext mockTopologyContext() {
        TopologyContext context = mock(TopologyContext.class);
        when(context.getStormId()).thenReturn("test-topology-1");
        when(context.getThisWorkerPort()).thenReturn(6700);
        return context;
    }

    @Test
    public void testFileRotation() throws IOException, InterruptedException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, tempDir.toAbsolutePath().toString());
        // We set rotation to be 1MB to trigger it easily, but we'll need to write
        // a lot. Alternatively, we can use a very small value, but we need an int >= 1.
        // Wait, Config is by MB. If we set it to 1, we still need to write 1MB.
        // Let's reflection inject a smaller value for tests? No, Storm uses config. 
        // We will just use `1` MB and write a large string a few times.
        conf.put(Config.TOPOLOGY_EVENTLOGGER_ROTATION_SIZE_MB, 1);
        conf.put(Config.TOPOLOGY_EVENTLOGGER_MAX_RETAINED_FILES, 2);

        eventLogger.prepare(conf, new HashMap<>(), mockTopologyContext());

        // 1 MB = 1048576 bytes
        // We create an event message that is about 100KB, write it 11 times to exceed 1MB.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            sb.append("A"); // 1 byte
        }
        String largeValue = sb.toString();

        List<Object> values = new ArrayList<>();
        values.add(largeValue);
        
        // This toString() will add some bytes overhead, so each event is ~ 100KB.
        IEventLogger.EventInfo eventInfo = new IEventLogger.EventInfo(
                System.currentTimeMillis(), "test-component", 1, "msgId", values);

        // Write 10 times -> ~1 MB
        for (int i = 0; i < 10; i++) {
            eventLogger.log(eventInfo);
        }

        // Wait a bit for flush if any (though rotation is synchronous in write)
        Thread.sleep(100);

        Path expectedLogDir = tempDir.resolve("test-topology-1").resolve("6700");
        Path logFile = expectedLogDir.resolve("events.log");
        Path logFile1 = expectedLogDir.resolve("events.log.1");
        Path logFile2 = expectedLogDir.resolve("events.log.2");

        // The first 10 writes should be in one file, almost 1 MB.
        assertTrue(Files.exists(logFile));
        
        // Write 2 more times to push it over 1MB
        eventLogger.log(eventInfo);
        eventLogger.log(eventInfo);
        
        Thread.sleep(100);

        // Now we expect events.log.1 to exist and events.log to be new
        assertTrue(Files.exists(logFile1), "Rotated file events.log.1 should exist");
        
        // Write 12 more times to push over 1MB again
        for (int i = 0; i < 12; i++) {
            eventLogger.log(eventInfo);
        }
        
        Thread.sleep(100);

        // Now events.log.2 and events.log.1 and events.log should exist
        assertTrue(Files.exists(logFile2), "Rotated file events.log.2 should exist");

        // Write 12 MORE times to push over 1MB again
        for (int i = 0; i < 12; i++) {
            eventLogger.log(eventInfo);
        }

        Thread.sleep(100);

        // max config was 2, so events.log.3 should NOT exist, and events.log.2 
        // should exist.
        Path logFile3 = expectedLogDir.resolve("events.log.3");
        assertTrue(!Files.exists(logFile3), "Rotated file events.log.3 should not exist");
        assertTrue(Files.exists(logFile2), "Rotated file events.log.2 should exist");
    }
}
