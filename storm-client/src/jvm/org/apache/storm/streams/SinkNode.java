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

package org.apache.storm.streams;

import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.utils.Utils;

/**
 * Sink node holds IRich or IBasic bolts that are passed via the {@code Stream#to()} api.
 */
class SinkNode extends Node {
    private final IComponent bolt;

    SinkNode(IComponent bolt) {
        super(Utils.DEFAULT_STREAM_ID, getOutputFields(bolt, Utils.DEFAULT_STREAM_ID));
        if (bolt instanceof IRichBolt || bolt instanceof IBasicBolt) {
            this.bolt = bolt;
        } else {
            throw new IllegalArgumentException("Should be an IRichBolt or IBasicBolt");
        }
    }

    IComponent getBolt() {
        return bolt;
    }
}
