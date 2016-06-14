/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares two shardIds based on their number after the '-'.
 */
public class ShardIdComparator implements Comparator<String>, Serializable {
    private static final long serialVersionUID = -5645750076315794599L;

    /** No-op constructor. */
    public ShardIdComparator() { }

    @Override
    public int compare(String shard1, String shard2) {
        Integer shard1Index = Integer.parseInt(shard1.split("-")[1]);
        Integer shard2Index = Integer.parseInt(shard2.split("-")[1]);
        return shard1Index.compareTo(shard2Index);
    }
}
