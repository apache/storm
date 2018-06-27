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

package org.apache.storm.ui.filters;

import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.ext.Provider;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class AuthorizedUserFilter implements ContainerRequestFilter {

    public static final Logger LOG = LoggerFactory.getLogger(AuthorizedUserFilter.class);
    public static Map<String, Object> conf = Utils.readStormConfig();
    public static IAuthorizer uiImpersonationHandler;
    public static IAuthorizer uiAclHandler;

    static {
        try {
            uiImpersonationHandler = StormCommon.mkAuthorizationHandler(
                        (String) conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
            uiAclHandler = StormCommon.mkAuthorizationHandler(
                    (String) conf.get(DaemonConfig.NIMBUS_AUTHORIZER), conf);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        List<PathSegment> matchedOps = containerRequestContext.getUriInfo().getPathSegments();

        matchedOps = matchedOps.subList(2, matchedOps.size());

        StringJoiner stringJoiner = new StringJoiner("/");
        for (PathSegment matchedOp : matchedOps) {
            stringJoiner.add(matchedOp.getPath());
        }

        String op = stringJoiner.toString();

        ReqContext reqContext = ReqContext.context();

        if (reqContext.isImpersonating()) {
            if (uiImpersonationHandler != null) {
                if (!uiImpersonationHandler.permit(reqContext, op, null)) {
                    Principal realPrincipal = reqContext.realPrincipal();
                    Principal principal = reqContext.principal();
                    String user = "unknown";
                    if (principal != null) {
                        user = principal.getName();
                    }
                    String realUser = "unknown";
                    if (realPrincipal != null) {
                        realUser = realPrincipal.getName();
                    }
                    InetAddress remoteAddress = reqContext.remoteAddress();

                    throw new IOException(
                            "user '" + realUser +  "' is not authorized to impersonate user '"
                            + user + "' from host '" + remoteAddress.toString() + "'. Please"
                            + "see SECURITY.MD to learn how to configure impersonation ACL."
                    );
                }

            LOG.warn(" principal " + reqContext.realPrincipal().toString() + " is trying to impersonate "
                            + reqContext.principal().toString() + " but "
                            + conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER)
                            + " has no authorizer configured. This is a potential security hole."
                            + "Please see SECURITY.MD to learn how to configure an impersonation authorizer.");
            }
        }

        if (uiAclHandler != null) {
            if (!uiAclHandler.permit(reqContext, op, null)) {
                Principal principal = reqContext.principal();
                String user = "unknown";
                if (principal != null) {
                    user = principal.getName();
                }
                throw new IOException("UI request '" + op + "' for '" + user + "' user is not authorized");
            }
        }
    }
}
