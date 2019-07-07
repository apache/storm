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

package org.apache.storm.metric;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;

/**
 * EventLogger interface for logging the event info to a sink like log file or db for inspecting the events via UI for debugging.
 */
public interface IEventLogger {

    void prepare(Map<String, Object> conf, Map<String, Object> arguments, TopologyContext context);

    /**
     * This method would be invoked when the {@link EventLoggerBolt} receives a tuple from the spouts or bolts that has event logging
     * enabled.
     *
     * @param e the event
     */
    void log(EventInfo e);

    void close();

    /**
     * A wrapper for the fields that we would log.
     */
    class EventInfo {
        private long ts;
        private String component;
        private int task;
        private Object messageId;
        private List<Object> values;

        public EventInfo(long ts, String component, int task, Object messageId, List<Object> values) {
            this.ts = ts;
            this.component = component;
            this.task = task;
            this.messageId = messageId;
            this.values = values;
        }

        public long getTs() {
            return ts;
        }

        public String getComponent() {
            return component;
        }

        public int getTask() {
            return task;
        }

        public Object getMessageId() {
            return messageId;
        }

        public List<Object> getValues() {
            return values;
        }

        /**
         * Returns a default formatted string with fields separated by ",".
         *
         * @return a default formatted string with fields separated by ","
         */
        @Override
        public String toString() {
            return new Date(ts).toString() + "," + component + "," + String.valueOf(task) + ","
                   + (messageId == null ? "" : messageId.toString()) + "," + values.toString();
        }
    }
}
