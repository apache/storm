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
package com.alibaba.jstorm.schedule;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class AssignmentBak implements Serializable {

    /**  */
    private static final long serialVersionUID = 7633746649144483965L;

    private final Map<String, List<Integer>> componentTasks;
    private final Assignment assignment;

    public AssignmentBak(Map<String, List<Integer>> componentTasks,
            Assignment assignment) {
        super();
        this.componentTasks = componentTasks;
        this.assignment = assignment;
    }

    public Map<String, List<Integer>> getComponentTasks() {
        return componentTasks;
    }

    public Assignment getAssignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
