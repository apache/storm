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

package org.apache.storm.daemon.ui.filters;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.Principal;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.common.JsonResponseBuilder;
import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.daemon.ui.resources.AuthNimbusOp;
import org.apache.storm.daemon.ui.resources.StormApiResource;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class AuthorizedUserFilter implements ContainerRequestFilter {

    public static final Logger LOG = LoggerFactory.getLogger(AuthorizedUserFilter.class);
    public static Map<String, Object> conf = Utils.readStormConfig();
    public static IAuthorizer uiImpersonationHandler;
    public static IAuthorizer uiAclHandler;

    @Context private ResourceInfo resourceInfo;

    static {
        try {
            uiImpersonationHandler = StormCommon.mkAuthorizationHandler(
                        (String) conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
            uiAclHandler = StormCommon.mkAuthorizationHandler(
                    (String) conf.get(DaemonConfig.NIMBUS_AUTHORIZER), conf);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            LOG.error("Error initializing AuthorizedUserFilter: ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * makeResponse.
     * @param ex ex
     * @param request request
     * @param statusCode statusCode
     * @return error response
     */
    public static Response makeResponse(Exception ex, ContainerRequestContext request, int statusCode) {
        String callback = null;

        if (request.getMediaType() != null && request.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE)) {
            try {
                String json = IOUtils.toString(request.getEntityStream(), Charsets.UTF_8);
                InputStream in = IOUtils.toInputStream(json);
                request.setEntityStream(in);
                Map<String, Object> requestBody = (Map<String, Object>) JSONValue.parse(json);
                if (requestBody.containsKey(StormApiResource.callbackParameterName)) {
                    callback = String.valueOf(requestBody.get(StormApiResource.callbackParameterName));
                }
            } catch (IOException e) {
                LOG.error("Exception while trying to get callback ", e);
            }
        }
        return new JsonResponseBuilder().setData(
                        UIHelpers.exceptionToJson(ex, statusCode)).setCallback(callback)
                        .setStatus(statusCode).build();
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
        AuthNimbusOp annotation = resourceInfo.getResourceMethod().getAnnotation(AuthNimbusOp.class);
        if (annotation == null) {
            return;
        }
        String op = annotation.value();
        if (op == null) {
            return;
        }

        Map topoConf = null;
        if (annotation.needsTopoId()) {
            final String topoId = containerRequestContext.getUriInfo().getPathParameters().get("id").get(0);
            try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf)) {
                topoConf = (Map) JSONValue.parse(nimbusClient.getClient().getTopologyConf(topoId));
            } catch (AuthorizationException ae) {
                LOG.error("Nimbus isn't allowing {} to access the topology conf of {}. {}", ReqContext.context(), topoId, ae.get_msg());
                containerRequestContext.abortWith(makeResponse(ae, containerRequestContext, 403));
                return;
            } catch (TException e) {
                LOG.error("Unable to fetch topo conf for {} due to ", topoId, e);
                containerRequestContext.abortWith(
                    makeResponse(new IOException("Unable to fetch topo conf for topo id " + topoId, e),
                            containerRequestContext, 500)
                );
                return;
            }
        }

        ReqContext reqContext = ReqContext.context();

        if (reqContext.isImpersonating()) {
            if (uiImpersonationHandler != null) {
                if (!uiImpersonationHandler.permit(reqContext, op, topoConf)) {
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

                    containerRequestContext.abortWith(
                            makeResponse(new AuthorizationException(
                                    "user '" + realUser +  "' is not authorized to impersonate user '"
                                            + user + "' from host '" + remoteAddress.toString() + "'. Please"
                                            + "see SECURITY.MD to learn how to configure impersonation ACL."
                            ), containerRequestContext, 401)
                    );
                    return;
                }

                LOG.warn(" principal {} is trying to impersonate {} but {} has no authorizer configured. "
                                + "This is a potential security hole. Please see SECURITY.MD to learn how to "
                                + "configure an impersonation authorizer.",
                        reqContext.realPrincipal().toString(), reqContext.principal().toString(),
                        conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER));
            }
        }

        if (uiAclHandler != null) {
            if (!uiAclHandler.permit(reqContext, op, topoConf)) {
                Principal principal = reqContext.principal();
                String user = "unknown";
                if (principal != null) {
                    user = principal.getName();
                }
                containerRequestContext.abortWith(
                        makeResponse(new AuthorizationException("UI request '" + op + "' for '"
                                        + user + "' user is not authorized"),
                                containerRequestContext, 403)
                );
                return;
            }
        }
    }
}
