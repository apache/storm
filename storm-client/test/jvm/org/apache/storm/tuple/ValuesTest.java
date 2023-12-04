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

import org.junit.Assert;
import org.junit.Test;

public class ValuesTest {

    @Test
    public void testNoArgsToValues() {
        Values vals = new Values();
        Assert.assertTrue("Failed to add null to Values", vals.size() == 0);
    }

    @Test
    public void testNullArgsToValues() {
        Values vals = new Values(null);
        Assert.assertTrue("Failed to add null to Values", vals.size() == 1);
        Assert.assertNull(vals.get(0));
    }

    @Test
    public void testNonNullArgsToValues() {
        Values vals = new Values("A", "B");
        Assert.assertTrue("Failed to Add values to Values", vals.size() == 2);
        Assert.assertEquals(vals.get(0), "A");
        Assert.assertEquals(vals.get(1), "B");
    }

    @Test
    public void testNullAsArgsToValues() {
        Values vals = new Values(null, "A");
        Assert.assertTrue("Failed to Add values to Values", vals.size() == 2);
        Assert.assertNull(vals.get(0));
        Assert.assertEquals(vals.get(1), "A");
    }
}
