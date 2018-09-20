/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.topology;

import org.apache.storm.generated.StormTopology;

public interface TestableTopology {
    String DUMMY_FIELD = "dummy";
    //Some tests rely on reading the worker log. If emits are too close together and too much is logged, the log might roll, breaking the test.
    int MIN_SLEEP_BETWEEN_EMITS_MS = 10;
    int MAX_SLEEP_BETWEEN_EMITS_MS = 100;
    StormTopology newTopology();
    String getBoltName();
    String getSpoutName();
}
