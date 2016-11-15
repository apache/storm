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

import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.SendResult;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.storm.druid.bolt.ITupleDruidEventMapper;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Trident {@link State} implementation for Druid.
 */
public class DruidBeamState<E> implements State {
    private static final Logger LOG = LoggerFactory.getLogger(DruidBeamState.class);

    private Beam<E> beam = null;
    private ITupleDruidEventMapper<E> druidEventMapper = null;

    public DruidBeamState(Beam<E> beam, ITupleDruidEventMapper<E> druidEventMapper) {
        this.beam = beam;
        this.druidEventMapper = druidEventMapper;
    }

    public  List<E> update(List<TridentTuple> tuples, TridentCollector collector) {
        List<E> events = new ArrayList<>(tuples.size());
        for (TridentTuple tuple: tuples) {
            events.add(druidEventMapper.getEvent(tuple));
        }

        LOG.info("Sending [{}] events", events.size());
        scala.collection.immutable.List<E> scalaList = scala.collection.JavaConversions.collectionAsScalaIterable(events).toList();
        Collection<Future<SendResult>> futureList = scala.collection.JavaConversions.asJavaCollection(beam.sendAll(scalaList));
        List<E> discardedEvents = new ArrayList<>();

        int index = 0;
        for (Future<SendResult> future : futureList) {
            try {
                SendResult result = Await.result(future);
                if (!result.sent()) {
                    discardedEvents.add(events.get(index));
                }
            } catch (Exception e) {
                LOG.error("Failed in writing messages to Druid", e);
            }
            index++;
        }

        return discardedEvents;

    }

    public void close() {
        try {
            Await.result(beam.close());
        } catch (Exception e) {
            LOG.error("Error while closing Druid beam client", e);
        }
    }

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }
}
