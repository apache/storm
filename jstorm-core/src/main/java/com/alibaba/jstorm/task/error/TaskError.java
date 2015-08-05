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
package com.alibaba.jstorm.task.error;

import java.io.Serializable;

/**
 * Task error stored in Zk(/storm-zk-root/taskerrors/{topologyid}/{taskid})
 * 
 * @author yannian
 * 
 */
public class TaskError implements Serializable {

    private static final long serialVersionUID = 5028789764629555542L;
    private String error;
    private int timSecs;

    public TaskError(String error, int timSecs) {
        this.error = error;
        this.timSecs = timSecs;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getTimSecs() {
        return timSecs;
    }

    public void setTimSecs(int timSecs) {
        this.timSecs = timSecs;
    }

}
