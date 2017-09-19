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

import java.util.Locale;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;

/**
 * The different types of groupings that are supported.
 */
public enum GroupingType {
    SHUFFLE {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.shuffleGrouping(stream.fromComponent, stream.id);
        }
    },
    FIELDS {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.fieldsGrouping(stream.fromComponent, stream.id, new Fields("key"));
        }
    },
    ALL {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.allGrouping(stream.fromComponent, stream.id);
        }
    },
    GLOBAL {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.globalGrouping(stream.fromComponent, stream.id);
        }
    },
    LOCAL_OR_SHUFFLE {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.localOrShuffleGrouping(stream.fromComponent, stream.id);
        }
    },
    NONE {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.noneGrouping(stream.fromComponent, stream.id);
        }
    },
    PARTIAL_KEY {
        @Override
        public void assign(BoltDeclarer declarer, InputStream stream) {
            declarer.partialKeyGrouping(stream.fromComponent, stream.id, new Fields("key"));
        }
    };

    /**
     * Parse a String config value and covert it into the enum.
     * @param conf the string config.
     * @return the parsed grouping type or SHUFFLE if conf is null.
     * @throws IllegalArgumentException if parsing does not work.
     */
    public static GroupingType fromConf(String conf) {
        String gt = "SHUFFLE";
        if (conf != null) {
            gt = conf.toUpperCase(Locale.ENGLISH);
        }
        return GroupingType.valueOf(gt);
    }

    public String toConf() {
        return toString();
    }

    public abstract void assign(BoltDeclarer declarer, InputStream stream);
}
