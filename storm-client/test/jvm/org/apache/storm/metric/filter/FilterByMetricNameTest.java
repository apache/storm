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

package org.apache.storm.metric.filter;

import java.util.List;
import java.util.Map;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterByMetricNameTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testWhitelist() {
        List<String> whitelistPattern = Lists.newArrayList("^metric\\.", "test\\.hello\\.[0-9]+");
        FilterByMetricName sut = new FilterByMetricName(whitelistPattern, null);

        Map<String, Boolean> testMetricNamesAndExpected = Maps.newHashMap();
        testMetricNamesAndExpected.put("storm.metric.hello", false);
        testMetricNamesAndExpected.put("test.hello.world", false);
        testMetricNamesAndExpected.put("test.hello.123", true);
        testMetricNamesAndExpected.put("test.metric.world", false);
        testMetricNamesAndExpected.put("metric.world", true);

        assertTests(sut, testMetricNamesAndExpected);
    }

    @Test
    public void testBlacklist() {
        List<String> blacklistPattern = Lists.newArrayList("^__", "test\\.");
        FilterByMetricName sut = new FilterByMetricName(null, blacklistPattern);

        Map<String, Boolean> testMetricNamesAndExpected = Maps.newHashMap();
        testMetricNamesAndExpected.put("__storm.metric.hello", false);
        testMetricNamesAndExpected.put("storm.metric.__hello", true);
        testMetricNamesAndExpected.put("test.hello.world", false);
        testMetricNamesAndExpected.put("storm.test.123", false);
        testMetricNamesAndExpected.put("metric.world", true);

        assertTests(sut, testMetricNamesAndExpected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBothWhitelistAndBlacklistAreSpecified() {
        List<String> whitelistPattern = Lists.newArrayList("^metric\\.", "test\\.hello\\.[0-9]+");
        List<String> blacklistPattern = Lists.newArrayList("^__", "test\\.");
        new FilterByMetricName(whitelistPattern, blacklistPattern);
    }

    @Test
    public void testNoneIsSpecified() {
        FilterByMetricName sut = new FilterByMetricName(null, null);

        Map<String, Boolean> testMetricNamesAndExpected = Maps.newHashMap();
        testMetricNamesAndExpected.put("__storm.metric.hello", true);
        testMetricNamesAndExpected.put("storm.metric.__hello", true);
        testMetricNamesAndExpected.put("test.hello.world", true);
        testMetricNamesAndExpected.put("storm.test.123", true);
        testMetricNamesAndExpected.put("metric.world", true);

        assertTests(sut, testMetricNamesAndExpected);
    }

    private void assertTests(FilterByMetricName sut, Map<String, Boolean> testMetricNamesAndExpected) {
        for (Map.Entry<String, Boolean> testEntry : testMetricNamesAndExpected.entrySet()) {
            assertEquals("actual filter result is not same: " + testEntry.getKey(),
                         testEntry.getValue(), sut.apply(new IMetricsConsumer.DataPoint(testEntry.getKey(), 1)));
        }
    }
}
