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
package com.alibaba.jstorm.task.group;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;

/**
 * user defined grouping method
 * 
 * @author Longda/yannian
 * 
 */
public class MkCustomGrouper {
    private CustomStreamGrouping grouping;

    private int myTaskId;

    public MkCustomGrouper(TopologyContext context,
            CustomStreamGrouping _grouping, GlobalStreamId stream,
            List<Integer> targetTask, int myTaskId) {
        this.myTaskId = myTaskId;
        this.grouping = _grouping;
        this.grouping.prepare(context, stream, targetTask);

    }

    public List<Integer> grouper(List<Object> values) {
        return this.grouping.chooseTasks(myTaskId, values);
    }
}
