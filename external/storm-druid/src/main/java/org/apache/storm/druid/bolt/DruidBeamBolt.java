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
package org.apache.storm.druid.bolt;

import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Basic bolt implementation for storing data to Druid datastore.
 * <p/>
 * This implementation uses Druid's Tranquility library (https://github.com/druid-io/tranquility)
 * to send to druid store.
 * Some of the concepts are borrowed from Tranquility storm connector implementation.
 * (https://github.com/druid-io/tranquility/blob/master/docs/storm.md)
 *
 * By default this Bolt expects to receive tuples in which "event" field gives your event type.
 * This logic can be changed by implementing ITupleDruidEventMapper interface.
 * <p/>
 *
 */
public class DruidBeamBolt<E> extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DruidBeamBolt.class);

    private volatile  OutputCollector collector;
    private DruidBeamFactory<E> beamFactory = null;
    private DruidConfig druidConfig = null;
    private Tranquilizer<E> tranquilizer = null;
    private ITupleDruidEventMapper<E> druidEventMapper = null;

    public DruidBeamBolt(DruidBeamFactory<E> beamFactory, ITupleDruidEventMapper<E> druidEventMapper, DruidConfig.Builder druidConfigBuilder) {
        this.beamFactory = beamFactory;
        this.druidConfig = druidConfigBuilder.build();
        this.druidEventMapper = druidEventMapper;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tranquilizer = Tranquilizer.builder()
                .maxBatchSize(druidConfig.getMaxBatchSize())
                .maxPendingBatches(druidConfig.getMaxPendingBatches())
                .lingerMillis(druidConfig.getLingerMillis())
                .blockOnFull(druidConfig.isBlockOnFull())
                .build(beamFactory.makeBeam(stormConf, context));
        this.tranquilizer.start();
    }

    @Override
    public void execute(final Tuple tuple) {
      Future future = tranquilizer.send((druidEventMapper.getEvent(tuple)));
        LOG.debug("Sent tuple : [{}]" , tuple);

        future.addEventListener(new FutureEventListener() {
          @Override
          public void onFailure(Throwable cause) {
              if (cause instanceof MessageDroppedException) {
                  collector.ack(tuple);
                  LOG.debug("Tuple Dropped due to MessageDroppedException : [{}]" , tuple);
                  if (druidConfig.getDiscardStreamId() != null)
                      collector.emit(druidConfig.getDiscardStreamId(), new Values(tuple, System.currentTimeMillis()));
              }
              else {
                  collector.fail(tuple);
                  LOG.debug("Tuple Processing Failed : [{}]" , tuple);
              }
          }

          @Override
          public void onSuccess(Object value) {
              collector.ack(tuple);
              LOG.debug("Tuple Processing Success : [{}]" , tuple);
          }
      });

    }

    @Override
    public void cleanup() {
        tranquilizer.stop();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(druidConfig.getDiscardStreamId(), new Fields("tuple", "timestamp"));
    }
}
