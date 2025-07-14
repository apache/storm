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

package org.apache.storm.tuple;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ValuesTest {

    @Test
    public void testNoArgsToValues() {
        Values vals = new Values();
        Assertions.assertEquals(0, vals.size(), "Failed to add null to Values");
    }

    @Test
    public void testNullArgsToValues() {
        Values vals = new Values(null);
        Assertions.assertEquals(1, vals.size(), "Failed to add null to Values");
        Assertions.assertNull(vals.get(0));
    }

    @Test
    public void testNonNullArgsToValues() {
        Values vals = new Values("A", "B");
        Assertions.assertEquals(2, vals.size(), "Failed to Add values to Values");
        Assertions.assertEquals(vals.get(0), "A");
        Assertions.assertEquals(vals.get(1), "B");
    }

    @Test
    public void testNullAsArgsToValues() {
        Values vals = new Values(null, "A");
        Assertions.assertEquals(2, vals.size(), "Failed to Add values to Values");
        Assertions.assertNull(vals.get(0));
        Assertions.assertEquals(vals.get(1), "A");
    }
}
