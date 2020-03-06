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

package org.apache.storm.daemon.drpc.webapp;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ReqContext;

@Provider
@PreMatching
public class ReqContextFilter implements Filter {
    private final IHttpCredentialsPlugin httpCredsHandler;

    public ReqContextFilter(IHttpCredentialsPlugin httpCredsHandler) {
        this.httpCredsHandler = httpCredsHandler;
    }
    
    /**
     * Populate the Storm RequestContext from an servlet request. This should be called in each handler
     * @param request the request to populate
     */
    public void populateContext(HttpServletRequest request) {
        if (httpCredsHandler != null) {
            httpCredsHandler.populateContext(ReqContext.context(), request);
        }
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
        //NOOP
        //We could add in configs through the web.xml if we wanted something stand alone here...
    }

    /**
     * A filter which populates the request if it is null and then passes it on to the next entity in the chain.
     * @param request the request to populate
     * @param response the response to populate
     * @param chain the next chain of entities to pass the object to
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        handle((HttpServletRequest) request, (HttpServletResponse) response, chain);
    }

    /**
     * A method used by doFilter which populates the request if it is null and then passes it on to the next entity in the chain.
     * @param request the request to populate
     * @param response the response to populate
     * @param chain the next chain of entities to pass the object to
     */
    public void handle(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (request != null) {
            populateContext(request);
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        //NOOP
    }
}
