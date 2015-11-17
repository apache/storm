/**
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
package storm.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SchemeAsMultiSchemeWithTopic implements MultiSchemeWithTopic {
    public static final String TOPIC_KEY = "topic";

    MultiScheme impl;
    String topicKey;

    public SchemeAsMultiSchemeWithTopic(Scheme impl) {
        this(impl, TOPIC_KEY);
    }
    public SchemeAsMultiSchemeWithTopic(Scheme impl, String topicKey) {
        this(new SchemeAsMultiScheme(impl), topicKey);
    }
    public SchemeAsMultiSchemeWithTopic(MultiScheme impl) {
        this(impl, TOPIC_KEY);
    }
    public SchemeAsMultiSchemeWithTopic(MultiScheme impl, String topicKey) {
        this.impl = impl;
        this.topicKey = topicKey;
    }


    @Override
    public Iterable<List<Object>> deserialize(ByteBuffer ser) {
        throw new NotImplementedException();
    }

    @Override
    public Iterable<List<Object>> deserializeWithTopic(final String topic, final ByteBuffer bytes) {
        return new Iterable<List<Object>>() {
            Iterable<List<Object>> baseIterable = impl.deserialize(bytes);
            @Override
            public Iterator<List<Object>> iterator() {
                final Iterator<List<Object>> baseIterator = baseIterable.iterator();
                return new Iterator<List<Object>>() {
                    @Override
                    public boolean hasNext() {
                        return baseIterator.hasNext();
                    }
                    @Override
                    public List<Object> next() {
                        List<Object> ret = new ArrayList<>();
                        ret.addAll(baseIterator.next());
                        ret.add(topic);
                        return ret;                        
                    }
                    @Override
                    public void remove() {
                        baseIterator.remove();
                    }
                };
            }
        };
    }

    @Override
    public Fields getOutputFields() {
        List<String> fields = new ArrayList<>();
        fields.addAll(impl.getOutputFields().toList());
        fields.add(topicKey);
        return new Fields(fields);
    }
}
