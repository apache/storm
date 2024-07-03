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
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import org.apache.storm.metric.StormMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
public class HeaderResponseFilter implements ContainerResponseFilter {
    public static final Logger LOG = LoggerFactory.getLogger(HeaderResponseFilter.class);

    private final Meter webRequestMeter;
    
    @Inject
    public HeaderResponseFilter(StormMetricsRegistry metricsRegistry) {
        this.webRequestMeter = metricsRegistry.registerMeter("num-web-requests");
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext,
                       ContainerResponseContext containerResponseContext) throws IOException {
        webRequestMeter.mark();
    }
}
