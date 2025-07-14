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

package org.apache.storm.redis.util;

import java.util.List;
import java.util.Objects;
import org.apache.storm.redis.util.outputcollector.EmittedTuple;
import org.apache.storm.tuple.Tuple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Utility for common test validations.
 */
public class TupleTestHelper {

    public static void verifyAnchors(final EmittedTuple emittedTuple, final Tuple expectedAnchor) {
        Objects.requireNonNull(emittedTuple);
        Objects.requireNonNull(expectedAnchor);
        assertEquals(1, emittedTuple.getAnchors().size(), "Should have a single anchor");

        final Tuple anchor = emittedTuple.getAnchors().get(0);
        assertNotNull(anchor, "Should be non-null");
        assertEquals(expectedAnchor, anchor);
    }

    public static void verifyEmittedTuple(final EmittedTuple emittedTuple, final List<Object> expectedValues) {
        Objects.requireNonNull(emittedTuple);
        Objects.requireNonNull(expectedValues);

        assertEquals(expectedValues.size(), emittedTuple.getTuple().size());
        assertEquals(expectedValues, emittedTuple.getTuple());
    }
}
