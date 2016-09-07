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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.druid.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DruidBeamStateUpdater<E> extends BaseStateUpdater<DruidBeamState<E>> {
    private static final Logger LOG = LoggerFactory.getLogger(DruidBeamStateUpdater.class);

    @Override
    public void updateState(DruidBeamState<E> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<E> discardedTuples = state.update(tuples, collector);
        processDiscardedTuples(discardedTuples);
    }

    /**
     * Users can override this method to  process the discarded Tuples
     * @param discardedTuples
     */
    protected void processDiscardedTuples(List<E> discardedTuples) {
        LOG.debug("discarded messages : [{}]" , discardedTuples);
    }

}
