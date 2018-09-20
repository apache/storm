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

package org.apache.storm.windowing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.storm.shade.com.google.common.collect.Iterators;
import org.apache.storm.tuple.Tuple;

/**
 * An iterator based implementation over the events in a window.
 */
public class TupleWindowIterImpl implements TupleWindow {
    private final Supplier<Iterator<Tuple>> tuplesIt;
    private final Supplier<Iterator<Tuple>> newTuplesIt;
    private final Supplier<Iterator<Tuple>> expiredTuplesIt;
    private final Long startTimestamp;
    private final Long endTimestamp;

    public TupleWindowIterImpl(Supplier<Iterator<Tuple>> tuplesIt,
                               Supplier<Iterator<Tuple>> newTuplesIt,
                               Supplier<Iterator<Tuple>> expiredTuplesIt,
                               Long startTimestamp, Long endTimestamp) {
        this.tuplesIt = tuplesIt;
        this.newTuplesIt = newTuplesIt;
        this.expiredTuplesIt = expiredTuplesIt;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    public List<Tuple> get() {
        List<Tuple> tuples = new ArrayList<>();
        tuplesIt.get().forEachRemaining(t -> tuples.add(t));
        return tuples;
    }

    @Override
    public Iterator<Tuple> getIter() {
        return Iterators.unmodifiableIterator(tuplesIt.get());
    }

    @Override
    public List<Tuple> getNew() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Tuple> getExpired() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }
}
