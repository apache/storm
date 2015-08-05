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
package com.alibaba.jstorm.zk;

import java.util.HashMap;

import org.apache.zookeeper.CreateMode;

public class ZkCreateModes {

    private static HashMap<CreateMode, String> map;

    static {
        map = new HashMap<CreateMode, String>();
        map.put(CreateMode.EPHEMERAL, ":ephemeral");
        map.put(CreateMode.PERSISTENT, ":persistent");
    }

    public static String getStateName(CreateMode mode) {
        return map.get(mode);
    }

}
