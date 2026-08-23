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

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.storm.daemon.ui.resources.AuthNimbusOp;
import org.apache.storm.daemon.ui.resources.UnauthenticatedNimbusOp;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizedUserFilterTest {

    /**
     * Stand-in for a resource class: one endpoint gated by an operation, one explicitly opted out,
     * and one carrying no annotation at all.
     */
    public static class SampleResource {
        @AuthNimbusOp("getNimbusConf")
        public void gated() {
        }

        @UnauthenticatedNimbusOp("filtered by Nimbus using the authenticated remote user")
        public void optedOut() {
        }

        public void unannotated() {
        }
    }

    private static AuthorizedUserFilter filterFor(String methodName, IAuthorizer aclHandler) throws Exception {
        Method method = SampleResource.class.getMethod(methodName);
        ResourceInfo resourceInfo = mock(ResourceInfo.class);
        when(resourceInfo.getResourceMethod()).thenReturn(method);

        AuthorizedUserFilter filter = new AuthorizedUserFilter();
        Field field = AuthorizedUserFilter.class.getDeclaredField("resourceInfo");
        field.setAccessible(true);
        field.set(filter, resourceInfo);

        AuthorizedUserFilter.uiAclHandler = aclHandler;
        AuthorizedUserFilter.uiImpersonationHandler = null;
        return filter;
    }

    private static int statusOfAbort(ContainerRequestContext request) {
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        verify(request).abortWith(response.capture());
        return response.getValue().getStatus();
    }

    @Test
    public void unannotatedEndpointIsDenied() throws Exception {
        IAuthorizer aclHandler = mock(IAuthorizer.class);
        when(aclHandler.permit(any(), any(), any())).thenReturn(true);
        AuthorizedUserFilter filter = filterFor("unannotated", aclHandler);
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        assertEquals(403, statusOfAbort(request));
        verify(aclHandler, never()).permit(any(), any(), any());
    }

    @Test
    public void annotatedEndpointIsCheckedAgainstItsOperation() throws Exception {
        IAuthorizer aclHandler = mock(IAuthorizer.class);
        when(aclHandler.permit(any(ReqContext.class), eq("getNimbusConf"), any())).thenReturn(false);
        AuthorizedUserFilter filter = filterFor("gated", aclHandler);
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        assertEquals(403, statusOfAbort(request));
        verify(aclHandler).permit(any(ReqContext.class), eq("getNimbusConf"), any());
    }

    @Test
    public void permittedEndpointIsNotAborted() throws Exception {
        IAuthorizer aclHandler = mock(IAuthorizer.class);
        when(aclHandler.permit(any(ReqContext.class), eq("getNimbusConf"), any())).thenReturn(true);
        AuthorizedUserFilter filter = filterFor("gated", aclHandler);
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        verify(request, never()).abortWith(any());
    }

    @Test
    public void explicitlyOptedOutEndpointSkipsTheAclHandler() throws Exception {
        IAuthorizer aclHandler = mock(IAuthorizer.class);
        AuthorizedUserFilter filter = filterFor("optedOut", aclHandler);
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        verify(request, never()).abortWith(any());
        verify(aclHandler, never()).permit(any(), any(), any());
    }

    @Test
    public void unannotatedEndpointIsDeniedEvenWithoutAnAclHandler() throws Exception {
        AuthorizedUserFilter filter = filterFor("unannotated", null);
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        assertEquals(403, statusOfAbort(request));
    }

    @Test
    public void everyApiEndpointDeclaresItsAuthorization() throws Exception {
        Class<?> resource = Class.forName("org.apache.storm.daemon.ui.resources.StormApiResource");
        for (Method method : resource.getDeclaredMethods()) {
            if (method.getAnnotation(jakarta.ws.rs.GET.class) == null
                && method.getAnnotation(jakarta.ws.rs.POST.class) == null) {
                continue;
            }
            boolean declared = method.getAnnotation(AuthNimbusOp.class) != null
                || method.getAnnotation(UnauthenticatedNimbusOp.class) != null;
            assertEquals(true, declared,
                method.getName() + " must declare @AuthNimbusOp or @UnauthenticatedNimbusOp");
        }
    }

    @Test
    public void clusterConfigurationRequiresTheNimbusConfOperation() throws Exception {
        Method method = Class.forName("org.apache.storm.daemon.ui.resources.StormApiResource")
            .getMethod("getClusterConfiguration", String.class);

        AuthNimbusOp annotation = method.getAnnotation(AuthNimbusOp.class);
        assertEquals("getNimbusConf", annotation == null ? null : annotation.value());
    }

    @Test
    public void topologyIdLookupIsSkippedForOptedOutEndpoints() throws Exception {
        AuthorizedUserFilter filter = filterFor("optedOut", mock(IAuthorizer.class));
        ContainerRequestContext request = mock(ContainerRequestContext.class);

        filter.filter(request);

        verify(request, never()).getUriInfo();
    }

    @Test
    public void aclHandlerReceivesNullTopologyConfForNonTopologyOperations() throws Exception {
        IAuthorizer aclHandler = mock(IAuthorizer.class);
        when(aclHandler.permit(any(), any(), any())).thenReturn(true);
        AuthorizedUserFilter filter = filterFor("gated", aclHandler);

        filter.filter(mock(ContainerRequestContext.class));

        ArgumentCaptor<Map> topoConf = ArgumentCaptor.forClass(Map.class);
        verify(aclHandler).permit(any(ReqContext.class), eq("getNimbusConf"), topoConf.capture());
        assertEquals(null, topoConf.getValue());
    }
}
