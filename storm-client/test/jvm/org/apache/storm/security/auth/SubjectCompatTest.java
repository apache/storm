/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth;

import java.io.IOException;
import javax.security.auth.Subject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SubjectCompatTest {

    @Test
    void currentSubjectReturnsNullWhenNoneSet() {
        assertNull(SubjectCompat.currentSubject());
    }

    @Test
    void doAsExecutesCallableAndReturnsResult() throws Exception {
        Subject subject = new Subject();
        String result = SubjectCompat.doAs(subject, () -> "hello");
        assertEquals("hello", result);
    }

    @Test
    void doAsWithNullSubject() throws Exception {
        String result = SubjectCompat.doAs(null, () -> "works");
        assertEquals("works", result);
    }

    @Test
    void doAsPropagatesCheckedException() {
        Subject subject = new Subject();
        assertThrows(IOException.class, () ->
            SubjectCompat.doAs(subject, () -> {
                throw new IOException("test error");
            })
        );
    }

    @Test
    void doAsPropagatesRuntimeException() {
        Subject subject = new Subject();
        assertThrows(IllegalStateException.class, () ->
            SubjectCompat.doAs(subject, () -> {
                throw new IllegalStateException("test error");
            })
        );
    }

    @Test
    void doAsSetsCurrentSubject() throws Exception {
        Subject subject = new Subject();
        Subject observed = SubjectCompat.doAs(subject, SubjectCompat::currentSubject);
        assertSame(subject, observed);
    }
}
