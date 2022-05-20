/*******************************************************************************
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
 *******************************************************************************/

package org.apache.storm.eventhubs.spout;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestEventHubSpout {

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testSpoutConfig() {
        EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "pas\\s+w/ord",
                                                           "namespace", "entityname", 16);
        conf.setZkConnectionString("zookeeper");
        conf.setCheckpointIntervalInSeconds(1);
        assertEquals(conf.getConnectionString(),
                     "Endpoint=amqps://namespace.servicebus.windows.net;EntityPath=entityname;SharedAccessKeyName=username;" +
                     "SharedAccessKey=pas\\s+w/ord;OperationTimeout=PT1M;RetryPolicy=Default");
    }

    @Test
    public void testSpoutBasic() {
        //This spout owns 2 partitions: 6 and 14
        EventHubSpoutCallerMock mock = new EventHubSpoutCallerMock(16, 8, 6, 10);
        String result = mock.execute("r6,f6_0,a6_1,a6_2,a14_0,a14_2,r4,f14_1,r2");
        assertEquals("6_0,14_0,6_1,14_1,6_2,14_2,6_0,14_3,6_3,14_4,6_4,14_1", result);
    }

    @Test
    public void testSpoutCheckpoint() {
        //Make sure that even though nextTuple() doesn't receive valid data,
        //the offset will be checkpointed after checkpointInterval seconds.

        //This spout owns 1 partitions: 6
        EventHubSpoutCallerMock mock = new EventHubSpoutCallerMock(8, 8, 6, 1);
        String result = mock.execute("r6,a6_0,a6_1,a6_2");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
        }
        EventHubReceiverMock.setPause(true);
        result = mock.execute("r3");
        EventHubReceiverMock.setPause(false);
        assertEquals("3", mock.getCheckpoint(6));
    }

}
