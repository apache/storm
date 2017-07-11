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

package org.apache.storm.daemon.utils;

import java.net.URLEncoder;
import java.util.Map;

import static java.util.stream.Collectors.joining;

public class URLBuilder {
    private URLBuilder() {
    }

    public static String build(String urlPath, Map<String, Object> parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(urlPath);
        if (parameters.size() > 0) {
            sb.append("?");

            String queryParam = parameters.entrySet().stream()
                    .map(entry -> URLEncoder.encode(entry.getKey()) + "=" + URLEncoder.encode(entry.getValue().toString()))
                    .collect(joining("&"));
            sb.append(queryParam);
        }
        return sb.toString();
    }
}
