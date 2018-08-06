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

import com.codahale.metrics.Meter;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.storm.metric.StormMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeaderResponseServletFilter implements Filter {
    public static final Logger LOG = LoggerFactory.getLogger(HeaderResponseServletFilter.class);

    private final Meter webRequestMeter;

    private final Meter mainPageRequestMeter;
    
    public HeaderResponseServletFilter(StormMetricsRegistry metricsRegistry) {
        this.webRequestMeter = metricsRegistry.registerMeter("num-web-requests");
        this.mainPageRequestMeter = metricsRegistry.registerMeter("ui:num-main-page-http-requests");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain filterChain) throws IOException, ServletException {
        webRequestMeter.mark();
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
        if ((httpRequest.getPathInfo()).equals("/index.html")) {
            mainPageRequestMeter.mark();
            httpResponse.addHeader("Cache-Control", "no-cache");
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {

    }
}
