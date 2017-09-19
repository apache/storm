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

package org.apache.storm.hdfs.security;

import org.apache.storm.common.AbstractHadoopAutoCreds;

import java.util.Map;

import static org.apache.storm.hdfs.security.HdfsSecurityUtil.HDFS_CREDENTIALS;

/**
 * Auto credentials plugin for HDFS implementation. This class provides a way to automatically
 * push credentials to a topology and to retrieve them in the worker.
 */
public class AutoHDFS extends AbstractHadoopAutoCreds {
    @Override
    public void doPrepare(Map conf) {
    }

    @Override
    protected String getConfigKeyString() {
        return HdfsSecurityUtil.HDFS_CREDENTIALS_CONFIG_KEYS;
    }

    @Override
    public String getCredentialKey(String configKey) {
        return HDFS_CREDENTIALS + configKey;
    }
}

