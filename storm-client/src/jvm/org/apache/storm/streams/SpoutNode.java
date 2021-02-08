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

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;

/**
 * A spout node wraps an {@link IRichSpout}.
 */
class SpoutNode extends Node {
    private final IRichSpout spout;

    SpoutNode(IRichSpout spout) {
        super(Utils.DEFAULT_STREAM_ID, getOutputFields(spout, Utils.DEFAULT_STREAM_ID));
        if (outputFields.size() == 0) {
            throw new IllegalArgumentException("Spout " + spout + " does not declare any fields"
                    + "for the stream '" + Utils.DEFAULT_STREAM_ID + "'");
        }
        this.spout = spout;
    }

    IRichSpout getSpout() {
        return spout;
    }

    @Override
    void addOutputStream(String streamId) {
        throw new UnsupportedOperationException("Cannot add output streams to a spout node");
    }

}
