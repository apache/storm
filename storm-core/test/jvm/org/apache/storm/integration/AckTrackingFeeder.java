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

package org.apache.storm.integration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.storm.testing.AckTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.tuple.Fields;

class AckTrackingFeeder {

    private final AckTracker tracker;
    private final FeederSpout spout;

    public AckTrackingFeeder(String... fields) {
        tracker = new AckTracker();
        spout = new FeederSpout(new Fields(fields));
        spout.setAckFailDelegate(tracker);
    }

    public FeederSpout getSpout() {
        return spout;
    }

    public void feed(List<Object> tuple) {
        spout.feed(tuple);
    }

    public void feed(List<Object> tuple, Object msgId) {
        spout.feed(tuple, msgId);
    }

    public void feedNoWait(List<Object> tuple, Object msgId) {
        spout.feedNoWait(tuple, msgId);
    }
    
    public void assertNumAcks(int expectedNumAcks) {
        assertThat(tracker.getNumAcks(), is(expectedNumAcks));
        tracker.resetNumAcks();
    }

}
