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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.opentsdb.client;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;

import org.apache.storm.opentsdb.OpenTsdbMetricDatapoint;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to connect to OpenTsdb TSD for storing timeseries datapoints.
 */
public class OpenTsdbClient {
    private static final String PUT_PATH = "/api/put";
    private static Logger LOG = LoggerFactory.getLogger(OpenTsdbClient.class);

    private final String urlString;
    private final boolean sync;
    private final long syncTimeout;
    private final ResponseType responseType;
    private final boolean enableChunkedEncoding;

    private WebTarget target;
    private Client client;

    public enum ResponseType {
        None(""),
        Summary("summary"),
        Details("details");

        private final String value;

        ResponseType(String value) {
            this.value = value;
        }
    }

    protected OpenTsdbClient(String urlString, boolean sync, long syncTimeOut, ResponseType responseType, boolean enableChunkedEncoding) {
        this.urlString = urlString;
        this.sync = sync;
        this.syncTimeout = syncTimeOut;
        this.responseType = responseType;
        this.enableChunkedEncoding = enableChunkedEncoding;

        init();
    }

    private void init() {

        final ApacheConnectorProvider apacheConnectorProvider = new ApacheConnectorProvider();
        final ClientConfig clientConfig = new ClientConfig().connectorProvider(apacheConnectorProvider);

        // transfer encoding should be set as jersey sets it on by default.
        clientConfig.property(ClientProperties.REQUEST_ENTITY_PROCESSING,
                enableChunkedEncoding ? RequestEntityProcessing.CHUNKED : RequestEntityProcessing.BUFFERED);

        client = ClientBuilder.newClient(clientConfig);

        target = client.target(urlString).path(PUT_PATH);
        if (sync) {
            // need to add an empty string else it is nto added as query param.
            target = target.queryParam("sync", "").queryParam("sync_timeout", syncTimeout);
        }
        if (responseType != ResponseType.None) {
            // need to add an empty string else it is nto added as query param.
            target = target.queryParam(responseType.value, "");
        }

        LOG.info("target uri [{}]", target.getUri());
    }

    public ClientResponse.Details writeMetricPoint(OpenTsdbMetricDatapoint metricDataPoint) {
        return target.request().post(Entity.json(metricDataPoint), ClientResponse.Details.class);
    }

    public ClientResponse.Details writeMetricPoints(Collection<OpenTsdbMetricDatapoint> metricDataPoints) {
        LOG.debug("Writing metric points to OpenTSDB [{}]", metricDataPoints.size());
        return target.request().post(Entity.json(metricDataPoints), ClientResponse.Details.class);
    }

    public void cleanup() {
        client.close();
    }

    public static OpenTsdbClient.Builder newBuilder(String url) {
        return new Builder(url);
    }

    public static class Builder implements Serializable {
        private final String url;
        private boolean sync;
        private long syncTimeOut;
        private boolean enableChunkedEncoding;
        private ResponseType responseType = ResponseType.None;

        public Builder(String url) {
            this.url = url;
        }

        public OpenTsdbClient.Builder sync(long timeoutInMilliSecs) {
            Preconditions.checkArgument(timeoutInMilliSecs > 0, "timeout value should be more than zero.");
            sync = true;
            syncTimeOut = timeoutInMilliSecs;
            return this;
        }

        public OpenTsdbClient.Builder returnSummary() {
            responseType = ResponseType.Summary;
            return this;
        }

        public OpenTsdbClient.Builder returnDetails() {
            responseType = ResponseType.Details;
            return this;
        }

        public Builder enableChunkedEncoding() {
            enableChunkedEncoding = true;
            return this;
        }

        public OpenTsdbClient build() {
            return new OpenTsdbClient(url, sync, syncTimeOut, responseType, enableChunkedEncoding);
        }
    }
}
