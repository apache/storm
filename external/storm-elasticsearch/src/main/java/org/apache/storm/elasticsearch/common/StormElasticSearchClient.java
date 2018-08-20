/**
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

package org.apache.storm.elasticsearch.common;

import java.io.Serializable;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Client for connecting Elasticsearch.
 */
public final class StormElasticSearchClient implements Serializable {

    private final EsConfig esConfig;

    public StormElasticSearchClient(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    /**
     * Creates a new {@link RestClient} using given {@link EsConfig}.
     *
     * @return {@link RestClient} for Elasticsearch connection
     */
    public RestClient construct() {
        RestClientBuilder builder = RestClient.builder(esConfig.getHttpHosts());
        if (esConfig.getMaxRetryTimeoutMillis() != null) {
            builder.setMaxRetryTimeoutMillis(esConfig.getMaxRetryTimeoutMillis());
        }
        if (esConfig.getDefaultHeaders() != null) {
            builder.setDefaultHeaders(esConfig.getDefaultHeaders());
        }
        if (esConfig.getFailureListener() != null) {
            builder.setFailureListener(esConfig.getFailureListener());
        }
        if (esConfig.getHttpClientConfigCallback() != null) {
            builder.setHttpClientConfigCallback(esConfig.getHttpClientConfigCallback());
        }
        if (esConfig.getRequestConfigCallback() != null) {
            builder.setRequestConfigCallback(esConfig.getRequestConfigCallback());
        }
        if (esConfig.getPathPrefix() != null) {
            builder.setPathPrefix(esConfig.getPathPrefix());
        }
        return builder.build();
    }
}
