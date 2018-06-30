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

package org.apache.storm.scheduler.utils;

/**
 * This class is used to trace the scheduling action in key point of master scheduling, see
 * Nimbus#mkAssignments for details.
 *
 * <p>Caution that this class is not thread safe, so it must be guarded by lock.
 */
public interface ISchedulingTracer {

    /**
     * Trace the action point by name.
     * @param pointName action name of key scheduling point.
     */
    void trace(String pointName);

    /**
     * Return the string presentation of the traced key points of current scheduling round.
     */
    String printActionTrace();

    /**
     * Return the string presentation of the traced key points of all scheduling rounds traced by this tracer.
     */
    String printAllActionTrace();

    /**
     * Clear all the internal state of this tracer.
     */
    void reset();

    /**
     * Keep all the internal points to tmp area and do a {@link #reset}.
     */
    void roll();
}
