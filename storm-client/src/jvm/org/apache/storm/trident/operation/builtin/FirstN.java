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

package org.apache.storm.trident.operation.builtin;

import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.Assembly;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;


/**
 * An {@link org.apache.storm.trident.operation.Assembly} implementation.
 */
public class FirstN implements Assembly {

    Aggregator agg;

    public FirstN(int n, String sortField) {
        this(n, sortField, false);
    }

    public FirstN(int n, String sortField, boolean reverse) {
        if (sortField != null) {
            agg = new FirstNSortedAgg(n, sortField, reverse);
        } else {
            agg = new FirstNAgg(n);
        }
    }

    @Override
    public Stream apply(Stream input) {
        Fields outputFields = input.getOutputFields();
        return input.partitionAggregate(outputFields, agg, outputFields)
                    .global()
                    .partitionAggregate(outputFields, agg, outputFields);
    }

    public static class FirstNAgg extends BaseAggregator<FirstNAgg.State> {
        int number;

        public FirstNAgg(int n) {
            number = n;
        }

        @Override
        public State init(Object batchId, TridentCollector collector) {
            return new State();
        }

        @Override
        public void aggregate(State val, TridentTuple tuple, TridentCollector collector) {
            if (val.emitted < number) {
                collector.emit(tuple);
                val.emitted++;
            }
        }

        @Override
        public void complete(State val, TridentCollector collector) {
        }

        static class State {
            int emitted = 0;
        }

    }

    public static class FirstNSortedAgg extends BaseAggregator<PriorityQueue> {

        int number;
        String sortField;
        boolean reverse;

        public FirstNSortedAgg(int n, String sortField, boolean reverse) {
            number = n;
            this.sortField = sortField;
            this.reverse = reverse;
        }

        @Override
        public PriorityQueue init(Object batchId, TridentCollector collector) {
            return new PriorityQueue(number, new Comparator<TridentTuple>() {
                @Override
                public int compare(TridentTuple t1, TridentTuple t2) {
                    Comparable c1 = (Comparable) t1.getValueByField(sortField);
                    Comparable c2 = (Comparable) t2.getValueByField(sortField);
                    int ret = c1.compareTo(c2);
                    if (reverse) {
                        ret *= -1;
                    }
                    return ret;
                }
            });
        }

        @Override
        public void aggregate(PriorityQueue state, TridentTuple tuple, TridentCollector collector) {
            state.add(tuple);
        }

        @Override
        public void complete(PriorityQueue val, TridentCollector collector) {
            int total = val.size();
            for (int i = 0; i < number && i < total; i++) {
                TridentTuple t = (TridentTuple) val.remove();
                collector.emit(t);
            }
        }
    }
}
