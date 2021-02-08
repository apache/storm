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

package org.apache.storm.clojure;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.StreamInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class ClojureSpout implements IRichSpout {
    Map<String, StreamInfo> fields;
    List<String> fnSpec;
    List<String> confSpec;
    List<Object> params;
    
    ISpout spout;
    
    public ClojureSpout(List fnSpec, List confSpec, List<Object> params, Map<String, StreamInfo> fields) {
        this.fnSpec = fnSpec;
        this.confSpec = confSpec;
        this.params = params;
        this.fields = fields;
    }
    

    @Override
    public void open(final Map<String, Object> conf, final TopologyContext context, final SpoutOutputCollector collector) {
        IFn hof = ClojureUtil.loadClojureFn(fnSpec.get(0), fnSpec.get(1));
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(params));
            final Map<Keyword, Object> collectorMap = new PersistentArrayMap(new Object[] {
                Keyword.intern(Symbol.create("output-collector")), collector,
                Keyword.intern(Symbol.create("context")), context});
            List<Object> args = new ArrayList<Object>() {
                {
                    add(conf);
                    add(context);
                    add(collectorMap);
                }
            };
            
            spout = (ISpout) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                spout.open(conf, context, collector);
            } catch (AbstractMethodError ame) {
                //ignore
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            spout.close();
        } catch (AbstractMethodError ame) {
            //ignore
        }
    }

    @Override
    public void nextTuple() {
        try {
            spout.nextTuple();
        } catch (AbstractMethodError ame) {
            //ignore
        }

    }

    @Override
    public void ack(Object msgId) {
        try {
            spout.ack(msgId);
        } catch (AbstractMethodError ame) {
            //ignore
        }

    }

    @Override
    public void fail(Object msgId) {
        try {
            spout.fail(msgId);
        } catch (AbstractMethodError ame) {
            //ignore
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (String stream: fields.keySet()) {
            StreamInfo info = fields.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        IFn hof = ClojureUtil.loadClojureFn(confSpec.get(0), confSpec.get(1));
        try {
            return (Map) hof.applyTo(RT.seq(params));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate() {
        try {
            spout.activate();
        } catch (AbstractMethodError ame) {
            //ignore
        }
    }

    @Override
    public void deactivate() {
        try {
            spout.deactivate();
        } catch (AbstractMethodError ame) {
            //ignore
        }
    }
}
