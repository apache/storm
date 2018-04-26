/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a mechanism to return results and messages from a scheduling strategy to the Resource Aware
 * Scheduler.
 */
public class SchedulingResult {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulingResult.class);

    //status of scheduling the topology e.g. success or fail?
    private final SchedulingStatus status;

    //arbitrary message to be returned when scheduling is done
    private final String message;

    //error message returned is something went wrong
    private final String errorMessage;

    private SchedulingResult(SchedulingStatus status, String message, String errorMessage) {
        this.status = status;
        this.message = message;
        this.errorMessage = errorMessage;
    }

    public static SchedulingResult failure(SchedulingStatus status, String errorMessage) {
        return new SchedulingResult(status, null, errorMessage);
    }

    public static SchedulingResult success() {
        return SchedulingResult.success(null);
    }

    public static SchedulingResult success(String message) {
        return new SchedulingResult(SchedulingStatus.SUCCESS, message, null);
    }

    public SchedulingStatus getStatus() {
        return this.status;
    }

    public String getMessage() {
        return this.message;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public boolean isSuccess() {
        return SchedulingStatus.isStatusSuccess(this.status);
    }

    public boolean isFailure() {
        return SchedulingStatus.isStatusFailure(this.status);
    }

    @Override
    public String toString() {
        String ret = null;
        if (isSuccess()) {
            ret = "Status: " + this.getStatus() + " message: " + this.getMessage();
        } else {
            ret = "Status: " + this.getStatus() + " error message: " + this.getErrorMessage();
        }
        return ret;
    }
}
