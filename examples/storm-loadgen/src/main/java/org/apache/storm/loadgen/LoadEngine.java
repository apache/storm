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

package org.apache.storm.loadgen;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.loadgen.CompStats;

/**
 * The goal of this class is to provide a set of "Tuples" to send that will match as closely as possible the characteristics
 * measured from a production topology.
 */
public class LoadEngine {

    //TODO need to do a lot...

    /**
     * Provides an API to simulate the timings and CPU utilization of a bolt or spout.
     */
    public static class InputTimingEngine {
        private final Random rand;
        private final CompStats stats;

        public InputTimingEngine(CompStats stats) {
            this.stats = stats;
            rand = ThreadLocalRandom.current();
        }
    }
}
