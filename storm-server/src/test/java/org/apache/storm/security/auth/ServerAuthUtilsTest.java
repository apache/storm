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

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.storm.DaemonConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class ServerAuthUtilsTest {

    public static class AuthUtilsTestMock implements IHttpCredentialsPlugin {

        @Override
        public void prepare(Map<String, Object> topoConf) {
            //NO OP
        }

        @Override
        public String getUserName(HttpServletRequest req) {
            return null;
        }

        @Override
        public ReqContext populateContext(ReqContext context, HttpServletRequest req) {
            return null;
        }
    }

    @Test
    public void uiHttpCredentialsPluginTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(
            DaemonConfig.UI_HTTP_CREDS_PLUGIN, AuthUtilsTestMock.class.getName());
        conf.put(
            DaemonConfig.DRPC_HTTP_CREDS_PLUGIN, AuthUtilsTestMock.class.getName());

        assertTrue(
            ServerAuthUtils.getUiHttpCredentialsPlugin(conf).getClass() == AuthUtilsTestMock.class);
        assertTrue(
            ServerAuthUtils.getDrpcHttpCredentialsPlugin(conf).getClass() == AuthUtilsTestMock.class);
    }
}