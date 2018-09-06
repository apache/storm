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

package org.apache.storm.daemon.ui;

import java.io.IOException;
import java.security.Principal;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple UI filter that should only be used for testing purposes.  To set a given user name,
 * set an init parameter of USER_NAME to be the name of the user you want the UI to always impersonate.
 */
public class TestingFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(TestingFilter.class);

    private String userName = "unknown_user";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String userName = filterConfig.getInitParameter("USER_NAME");
        if (userName != null) {
            this.userName = userName;
            LOG.info("Will use {} as the user name for all http requests", userName);
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException {
        ServletRequest filteredRequest = new HttpServletRequestWrapper((HttpServletRequest) servletRequest) {
            @Override
            public String getRemoteUser() {
                return userName;
            }

            @Override
            public Principal getUserPrincipal() {
                return () -> userName;
            }
        };
        LOG.debug("Changing user name to {}", userName);
        filterChain.doFilter(filteredRequest, servletResponse);
    }

    @Override
    public void destroy() {
        //NOOP
    }
}
