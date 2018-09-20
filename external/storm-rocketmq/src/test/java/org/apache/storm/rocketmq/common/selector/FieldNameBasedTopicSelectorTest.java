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

package org.apache.storm.rocketmq.common.selector;

import org.apache.storm.rocketmq.TestUtils;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldNameBasedTopicSelectorTest {
    @Test
    public void getTopic() throws Exception {
        FieldNameBasedTopicSelector topicSelector = new FieldNameBasedTopicSelector("f1", "tpc", "f2",  "tg");
        Tuple tuple = TestUtils.generateTestTuple("f1", "fn", "v1", "vn");
        assertEquals("v1", topicSelector.getTopic(tuple));
        assertEquals("tg", topicSelector.getTag(tuple));

        tuple = TestUtils.generateTestTuple("fn", "f2", "vn", "v2");
        assertEquals("tpc", topicSelector.getTopic(tuple));
        assertEquals("v2", topicSelector.getTag(tuple));
    }

}