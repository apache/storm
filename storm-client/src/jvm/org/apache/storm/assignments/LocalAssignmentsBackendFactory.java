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

package org.apache.storm.assignments;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.base.Preconditions;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ReflectionUtils;

/**
 * Factory class for creating local assignments.
 */
public class LocalAssignmentsBackendFactory {

    public static ILocalAssignmentsBackend getBackend(Map<String, Object> conf) {
        if (conf.get(Config.NIMBUS_LOCAL_ASSIGNMENTS_BACKEND_CLASS) != null) {
            Object targetObj = ReflectionUtils.newInstance((String) conf.get(Config.NIMBUS_LOCAL_ASSIGNMENTS_BACKEND_CLASS));
            Preconditions.checkState(targetObj instanceof ILocalAssignmentsBackend, "{} must implements ILocalAssignmentsBackend",
                                     Config.NIMBUS_LOCAL_ASSIGNMENTS_BACKEND_CLASS);
            ((ILocalAssignmentsBackend) targetObj).prepare(conf);
            return (ILocalAssignmentsBackend) targetObj;
        }

        return getDefault();
    }

    public static ILocalAssignmentsBackend getDefault() {
        ILocalAssignmentsBackend backend = new InMemoryAssignmentBackend();
        backend.prepare(ConfigUtils.readStormConfig());
        return backend;
    }
}
