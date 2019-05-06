/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm;

import static org.apache.storm.utils.PredicateMatcher.matchesPredicate;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.storm.testing.AckFailMapTracker;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

public class AssertLoop {

    public static void assertLoop(Predicate<Object> condition, Object... conditionParams) {
        try {
            Awaitility.with()
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(Arrays.asList(conditionParams), everyItem(matchesPredicate(condition))));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    public static void assertAcked(AckFailMapTracker tracker, Object... ids) {
        assertLoop(tracker::isAcked, ids);
    }

    public static void assertFailed(AckFailMapTracker tracker, Object... ids) {
        assertLoop(tracker::isFailed, ids);
    }
    
}
