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
package backtype.storm.security.auth;

import backtype.storm.utils.Utils;
import backtype.storm.Config;

import java.util.Map;

/**
 * The purpose for which the Thrift server is created.
 */
public enum ThriftConnectionType {
    NIMBUS("nimbus.thrift"),
    DRPC("drpc.worker");

    private final String configPrefix;

    ThriftConnectionType(String pfx) {
        this.configPrefix = pfx;
    }

    public int getNumThreads(Map conf) { 
        return Utils.getInt(conf.get(this.configPrefix + ".threads"));
    }

    public int getMaxBufferSize(Map conf) {
        return Utils.getInt(conf.get(this.configPrefix + ".max_buffer_size"));
    }
}
