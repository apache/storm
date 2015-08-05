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

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * The callback will be called, when task occur error It just call
 * TaskReportErrorAndDie
 * 
 * @author yannian
 * 
 */
public class TaskErrorRunable extends RunnableCallback {
    private TaskReportErrorAndDie report_error_and_die;

    public TaskErrorRunable(TaskReportErrorAndDie _report_error_and_die) {
        this.report_error_and_die = _report_error_and_die;
    }

    @Override
    public <T> Object execute(T... args) {
        Exception e = null;
        if (args != null && args.length > 0) {
            e = (Exception) args[0];
        }
        if (e != null) {
            report_error_and_die.report(e);
        }
        return null;
    }

}
