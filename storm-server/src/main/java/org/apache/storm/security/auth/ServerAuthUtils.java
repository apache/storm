/*
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

package org.apache.storm.security.auth;

import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.ReflectionUtils;

public class ServerAuthUtils {
    public static IHttpCredentialsPlugin getHttpCredentialsPlugin(Map<String, Object> conf,
                                                                  String klassName) {
        try {
            IHttpCredentialsPlugin plugin = null;
            if (StringUtils.isNotBlank(klassName)) {
                plugin = ReflectionUtils.newInstance(klassName);
                plugin.prepare(conf);
            }
            return plugin;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the UI storm configuration.
     *
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin getUiHttpCredentialsPlugin(Map<String, Object> conf) {
        String klassName = (String) conf.get(DaemonConfig.UI_HTTP_CREDS_PLUGIN);
        return getHttpCredentialsPlugin(conf, klassName);
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the DRPC storm configuration.
     *
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin getDrpcHttpCredentialsPlugin(Map<String, Object> conf) {
        String klassName = (String) conf.get(DaemonConfig.DRPC_HTTP_CREDS_PLUGIN);
        return klassName == null ? null : getHttpCredentialsPlugin(conf, klassName);
    }
}
