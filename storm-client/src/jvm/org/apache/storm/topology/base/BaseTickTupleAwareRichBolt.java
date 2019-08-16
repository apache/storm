/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.topology.base;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

/**
 * This class is based on BaseRichBolt, but is aware of tick tuple.
 */
public abstract class BaseTickTupleAwareRichBolt extends BaseRichBolt {
    /**
     * {@inheritDoc}
     *
     * @param tuple the tuple to process.
     */
    @Override
    public void execute(final Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            onTickTuple(tuple);
        } else {
            process(tuple);
        }
    }

    /**
     * Process a single tick tuple of input. Tick tuple doesn't need to be acked. It provides default "DO NOTHING" implementation for
     * convenient. Override this method if needed.
     *
     * <p>More details on {@link org.apache.storm.task.IBolt#execute(Tuple)}.
     *
     * @param tuple The input tuple to be processed.
     */
    protected void onTickTuple(final Tuple tuple) {
    }

    /**
     * Process a single non-tick tuple of input. Implementation needs to handle ack manually. More details on {@link
     * org.apache.storm.task.IBolt#execute(Tuple)}.
     *
     * @param tuple The input tuple to be processed.
     */
    protected abstract void process(Tuple tuple);
}
