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

package org.apache.storm.utils;

import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.task.TopologyContext;

/**
 * Handle logging from non-JVM processes.
 */
public interface ShellLogHandler {

    /**
     * Called at least once before {@link ShellLogHandler#log} for each spout and bolt. Allows implementing classes to save information
     * about the current running context e.g. pid, thread, task.
     *
     * @param ownerCls - the class which instantiated this ShellLogHandler.
     * @param process  - the current {@link ShellProcess}.
     * @param context  - the current {@link TopologyContext}.
     */
    void setUpContext(Class<?> ownerCls, ShellProcess process,
                      TopologyContext context);

    /**
     * Called by spouts and bolts when they receive a 'log' command from a multilang process.
     *
     * @param msg - the {@link ShellMsg} containing the message to log.
     */
    void log(ShellMsg msg);
}