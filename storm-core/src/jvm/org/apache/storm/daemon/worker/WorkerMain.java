/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.worker;

import com.google.common.base.Preconditions;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Entry class for launching worker daemon
 */
public class WorkerMain {
    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 4, "Illegal number of arguemtns. Expected: 4, Actual: " + args.length);
        String stormId = args[0];
        String assignmentId = args[1];
        String portStr = args[2];
        String workerId = args[3];
        Map conf = Utils.readStormConfig();
        Utils.setupDefaultUncaughtExceptionHandler();
        StormCommon.validateDistributedMode(conf);
        Worker worker = new Worker(conf, null, stormId, assignmentId, Integer.parseInt(portStr), workerId);
        worker.start();
        Utils.addShutdownHookWithForceKillIn1Sec(worker::shutdown);
    }
}
