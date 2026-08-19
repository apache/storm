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

package org.apache.storm.daemon.logviewer.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import jakarta.ws.rs.core.Response;

import java.util.Collections;

import org.junit.jupiter.api.Test;

public class LogviewerResponseBuilderTest {

    /**
     * A success response must keep the documented Access-Control-Allow-Origin: * and must not
     * echo the request origin back, nor allow credentials.
     */
    @Test
    public void testSuccessJsonResponseDoesNotEchoRequestOrigin() {
        Response response = LogviewerResponseBuilder.buildSuccessJsonResponse(
                Collections.singletonMap("someKey", "someValue"), null, "http://other.example.com");

        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaderString("Access-Control-Allow-Origin"), is("*"));
        assertThat(response.getHeaderString("Access-Control-Allow-Credentials"), is(nullValue()));
    }
}
