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

package org.apache.storm.security.auth;

import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultHttpCredentialsPlugin implements IHttpCredentialsPlugin {
    private static final Logger LOG =
        LoggerFactory.getLogger(DefaultHttpCredentialsPlugin.class);

    /**
     * No-op.
     *
     * @param topoConf Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> topoConf) {
        // Do nothing.
    }

    @Override
    public String getUserName(jakarta.servlet.http.HttpServletRequest req) {
        String ret = null;
        if (req != null) {
            Principal princ = req.getUserPrincipal();
            if (princ != null) {
                ret = princ.getName();
            }

            if (ret != null && !ret.isEmpty()) {
                LOG.debug("Get user name {} from http request principal", ret);
            } else {
                ret = req.getRemoteUser();
                if (ret != null && !ret.isEmpty()) {
                    LOG.debug("Get user name {} from http request remote user", ret);
                }
            }
        }
        return ret;
    }

    @Override
    public ReqContext populateContext(ReqContext context, jakarta.servlet.http.HttpServletRequest req) {
        String userName = getUserName(req);

        String doAsUser = req.getHeader("doAsUser");
        if (doAsUser == null) {
            doAsUser = req.getParameter("doAsUser");
        }

        if (doAsUser != null) {
            context.setRealPrincipal(new SingleUserPrincipal(userName));
            userName = doAsUser;
        } else {
            context.setRealPrincipal(null);
        }

        Set<Principal> principals = new HashSet<>();
        if (userName != null) {
            Principal p = new SingleUserPrincipal(userName);
            principals.add(p);
        }
        Subject s = new Subject(true, principals, new HashSet(), new HashSet());
        context.setSubject(s);

        return context;
    }
}
