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

package org.apache.storm.executor;

import java.io.Serializable;
import java.util.List;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;

public class TupleInfo implements Serializable {

    private static final long serialVersionUID = -3348670497595864118L;

    private int taskId;
    private Object messageId;
    private String stream;
    private List<Object> values;
    private long timestamp;
    private long rootId;

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object messageId) {
        this.messageId = messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                                                  ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public long getRootId() {
        return rootId;
    }

    public void setRootId(long rootId) {
        this.rootId = rootId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void clear() {
        messageId = null;
        stream = null;
        values = null;
        timestamp = 0;
        rootId = 0;
    }
}
