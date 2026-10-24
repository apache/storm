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
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Test
    public void testDownloadFileClosesInputAfterWrite() throws IOException {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream is = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() {
                closed.set(true);
            }
        };
        Response response = LogviewerResponseBuilder.buildDownloadFile("file.log", is);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ((StreamingOutput) response.getEntity()).write(out);

        assertThat(out.toString(StandardCharsets.UTF_8), is("content"));
        assertThat(closed.get(), is(true));
    }

    @Test
    public void testDownloadFileClosesInputWhenWriteFails() {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream is = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() {
                closed.set(true);
            }
        };
        Response response = LogviewerResponseBuilder.buildDownloadFile("file.log", is);
        //Models a client that aborts the download
        OutputStream failing = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("aborted");
            }
        };

        assertThrows(IOException.class, () -> ((StreamingOutput) response.getEntity()).write(failing));
        assertThat(closed.get(), is(true));
    }
}
