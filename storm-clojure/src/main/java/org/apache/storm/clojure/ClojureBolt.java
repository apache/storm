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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.coordination.CoordinatedBolt.FinishedCallback;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Symbol;

public class ClojureBolt implements IRichBolt, FinishedCallback {
    Map<String, StreamInfo> _fields;
    List<String> _fnSpec;
    List<String> _confSpec;
    List<Object> _params;

    IBolt _bolt;

    public ClojureBolt(List fnSpec, List confSpec, List<Object> params, Map<String, StreamInfo> fields) {
        _fnSpec = fnSpec;
        _confSpec = confSpec;
        _params = params;
        _fields = fields;
    }

    @Override
    public void prepare(final Map<String, Object> topoConf, final TopologyContext context, final OutputCollector collector) {
        IFn hof = ClojureUtil.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
            final Map<Keyword,Object> collectorMap = new PersistentArrayMap( new Object[] {
                    Keyword.intern(Symbol.create("output-collector")), collector,
                    Keyword.intern(Symbol.create("context")), context});
            List<Object> args = new ArrayList<Object>() {{
                add(topoConf);
                add(context);
                add(collectorMap);
            }};

            _bolt = (IBolt) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                _bolt.prepare(topoConf, context, collector);
            } catch(AbstractMethodError ame) {

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        _bolt.execute(new ClojureTuple(input));
    }

    @Override
    public void cleanup() {
        try {
            _bolt.cleanup();
        } catch(AbstractMethodError ame) {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: _fields.keySet()) {
            StreamInfo info = _fields.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }

    @Override
    public void finishedId(Object id) {
        if(_bolt instanceof FinishedCallback) {
            ((FinishedCallback) _bolt).finishedId(id);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        IFn hof = ClojureUtil.loadClojureFn(_confSpec.get(0), _confSpec.get(1));
        try {
            return (Map) hof.applyTo(RT.seq(_params));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
