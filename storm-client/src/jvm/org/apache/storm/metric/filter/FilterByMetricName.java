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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.shade.com.google.common.cache.Cache;
import org.apache.storm.shade.com.google.common.cache.CacheBuilder;
import org.apache.storm.shade.com.google.common.collect.Iterators;
import org.apache.storm.shade.com.google.common.collect.Lists;

public class FilterByMetricName implements MetricsFilter {
    private final Cache<String, Boolean> filterCache;
    private final List<Pattern> whitelistPattern;
    private final List<Pattern> blacklistPattern;
    private boolean noneSpecified = false;

    public FilterByMetricName(List<String> whitelistPattern, List<String> blacklistPattern) {
        // guard NPE
        if (whitelistPattern == null) {
            this.whitelistPattern = Collections.emptyList();
        } else {
            this.whitelistPattern = convertPatternStringsToPatternInstances(whitelistPattern);
        }

        // guard NPE
        if (blacklistPattern == null) {
            this.blacklistPattern = Collections.emptyList();
        } else {
            this.blacklistPattern = convertPatternStringsToPatternInstances(blacklistPattern);
        }

        if (this.whitelistPattern.isEmpty() && this.blacklistPattern.isEmpty()) {
            noneSpecified = true;
        } else if (!this.whitelistPattern.isEmpty() && !this.blacklistPattern.isEmpty()) {
            throw new IllegalArgumentException("You have to specify either includes or excludes, or none.");
        }

        filterCache = CacheBuilder.newBuilder()
                                  .maximumSize(1000)
                                  .build();
    }

    @Override
    public boolean apply(IMetricsConsumer.DataPoint dataPoint) {
        if (noneSpecified) {
            return true;
        }

        String metricName = dataPoint.name;

        Boolean cachedFilteredIn = filterCache.getIfPresent(metricName);
        if (cachedFilteredIn != null) {
            return cachedFilteredIn;
        } else {
            boolean filteredIn = isFilteredIn(metricName);
            filterCache.put(metricName, filteredIn);
            return filteredIn;
        }
    }

    private ArrayList<Pattern> convertPatternStringsToPatternInstances(List<String> patterns) {
        return Lists.newArrayList(Iterators.transform(patterns.iterator(), s -> Pattern.compile(s)));
    }

    private boolean isFilteredIn(String metricName) {
        if (!whitelistPattern.isEmpty()) {
            return checkMatching(metricName, whitelistPattern, true);
        } else if (!blacklistPattern.isEmpty()) {
            return checkMatching(metricName, blacklistPattern, false);
        }

        throw new IllegalStateException("Shouldn't reach here");
    }

    private boolean checkMatching(String metricName, List<Pattern> patterns, boolean valueWhenMatched) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(metricName).find()) {
                return valueWhenMatched;
            }
        }

        return !valueWhenMatched;
    }
}
