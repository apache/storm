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
 *
 */

package org.apache.storm.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleVersionTest {

    @Test
    public void testParseStorm2x() {
        SimpleVersion version = new SimpleVersion("2.1.2");
        assertEquals(2, version.getMajor());
        assertEquals(1, version.getMinor());
    }

    @Test
    public void testParseStorm2xSnapshot() {
        SimpleVersion version = new SimpleVersion("2.1.2-SNAPSHOT");
        assertEquals(2, version.getMajor());
        assertEquals(1, version.getMinor());
    }

    @Test
    public void testParseStorm1x() {
        SimpleVersion version = new SimpleVersion("1.0.4");
        assertEquals(1, version.getMajor());
        assertEquals(0, version.getMinor());
    }

    @Test
    public void testParseStorm1xSnapshot() {
        SimpleVersion version = new SimpleVersion("1.0.4-SNAPSHOT");
        assertEquals(1, version.getMajor());
        assertEquals(0, version.getMinor());
    }

    @Test
    public void testParseStorm0x() {
        SimpleVersion version = new SimpleVersion("0.10.3");
        assertEquals(0, version.getMajor());
        assertEquals(10, version.getMinor());
    }

    @Test
    public void testParseStorm0xSnapshot() {
        SimpleVersion version = new SimpleVersion("0.10.3-SNAPSHOT");
        assertEquals(0, version.getMajor());
        assertEquals(10, version.getMinor());
    }

}