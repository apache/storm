/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.elasticsearch.common;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;

/**
 * Configuration for Elasticsearch connection.
 * @since 0.11
 */
public class EsConfig implements Serializable {

    private final HttpHost[] httpHosts;
    private Integer maxRetryTimeoutMillis;
    private Header[] defaultHeaders;
    private RestClient.FailureListener failureListener;
    private HttpClientConfigCallback httpClientConfigCallback;
    private RequestConfigCallback requestConfigCallback;
    private String pathPrefix;

    /**
    * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory.
    * Connects to Elasticsearch at http://localhost:9200.
    */
    public EsConfig() {
        this("http://localhost:9200");
    }

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory.
     *
     * @param urls Elasticsearch addresses in scheme://host:port pattern string array
     * @throws IllegalArgumentException if urls are empty
     * @throws NullPointerException     on any of the fields being null
     */
    public EsConfig(String... urls) {
        if (urls.length == 0) {
            throw new IllegalArgumentException("urls is required");
        }
        this.httpHosts = new HttpHost[urls.length];
        for (int i = 0; i < urls.length; i++) {
            URI uri = toUri(urls[i]);
            this.httpHosts[i] = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        }
    }

    static URI toUri(String url) throws IllegalArgumentException {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url " + url);
        }
    }
    
    public EsConfig withMaxRetryTimeoutMillis(Integer maxRetryTimeoutMillis) {
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        return this;
    }

    public EsConfig withDefaultHeaders(Header[] defaultHeaders) {
        this.defaultHeaders = defaultHeaders;
        return this;
    }

    public EsConfig withFailureListener(RestClient.FailureListener failureListener) {
        this.failureListener = failureListener;
        return this;
    }

    public EsConfig withHttpClientConfigCallback(HttpClientConfigCallback httpClientConfigCallback) {
        this.httpClientConfigCallback = httpClientConfigCallback;
        return this;
    }

    public EsConfig withRequestConfigCallback(RequestConfigCallback requestConfigCallback) {
        this.requestConfigCallback = requestConfigCallback;
        return this;
    }

    public EsConfig withPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
        return this;
    }

    public HttpHost[] getHttpHosts() {
        return httpHosts;
    }

    public Integer getMaxRetryTimeoutMillis() {
        return maxRetryTimeoutMillis;
    }

    public Header[] getDefaultHeaders() {
        return defaultHeaders;
    }

    public RestClient.FailureListener getFailureListener() {
        return failureListener;
    }

    public HttpClientConfigCallback getHttpClientConfigCallback() {
        return httpClientConfigCallback;
    }

    public RequestConfigCallback getRequestConfigCallback() {
        return requestConfigCallback;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }
}
