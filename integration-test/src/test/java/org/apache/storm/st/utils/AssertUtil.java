/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssertUtil {
    private static final Logger log = LoggerFactory.getLogger(AssertUtil.class);

    public static void empty(Collection<?> collection) {
        assertTrue(collection == null || collection.size() == 0, "Expected collection to be non-null, found: " + collection);
    }

    public static void nonEmpty(Collection<?> collection, String message) {
        assertNotNull(collection, message + " Expected collection to be non-null, found: " + collection);
        greater(collection.size(), 0, message + " Expected collection to be non-empty, found: " + collection);
    }

    public static void greater(int actual, int expected, String message) {
        assertTrue(actual > expected, message);
    }

    public static void exists(File path) {
        assertNotNull(path, "Supplied path was expected to be non null, found: " + path);
        assertTrue(path.exists(), "Supplied path was expected to be non null, found: " + path);
    }

    public static void assertOneElement(Collection<?> collection) {
        assertNElements(collection, 1);
    }

    public static void assertNElements(Collection<?> collection, int expectedCount) {
        String message = "Unexpected number of elements in the collection: " + collection;
        assertEquals(collection.size(), expectedCount, message);
    }

    public static void assertTwoElements(Collection<?> collection) {
        assertNElements(collection, 2);
    }

    public static void assertMatchCount(String actualOutput, List<String> expectedOutput, int requiredMatchCount) {
        for (String oneExpectedOutput : expectedOutput) {
            final int matchCount = StringUtils.countMatches(actualOutput, oneExpectedOutput);
            log.info("In output, found " + matchCount + " occurrences of: " + oneExpectedOutput);
            assertTrue(matchCount > requiredMatchCount,
                    "Found " + matchCount + "occurrence of " + oneExpectedOutput + " in urls, expected" + requiredMatchCount);
        }
    }
}
