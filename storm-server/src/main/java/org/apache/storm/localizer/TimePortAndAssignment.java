/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.localizer;

import com.codahale.metrics.Timer;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.metric.timed.Timed;

public class TimePortAndAssignment extends Timed<PortAndAssignment> implements PortAndAssignment {

    public TimePortAndAssignment(PortAndAssignment measured, Timer timer) {
        super(measured, timer);
    }

    @Override
    public String getToplogyId() {
        return getMeasured().getToplogyId();
    }

    @Override
    public String getOwner() {
        return getMeasured().getOwner();
    }

    @Override
    public int getPort() {
        return getMeasured().getPort();
    }

    @Override
    public LocalAssignment getAssignment() {
        return getMeasured().getAssignment();
    }

    @Override
    public void complete() {
        stopTiming();
    }
}
