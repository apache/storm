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

package org.apache.storm.mongodb.trident.state;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.MongoDbClient;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoMapState<T> implements IBackingMap<T> {
    private static Logger LOG = LoggerFactory.getLogger(MongoMapState.class);

    @SuppressWarnings("rawtypes")
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    private Options<T> options;
    private Serializer<T> serializer;
    private MongoDbClient mongoClient;
    private Map<String, Object> map;

    protected MongoMapState(Map<String, Object> map, Options options) {
        this.options = options;
        this.map = map;
        this.serializer = options.serializer;

        Validate.notEmpty(options.url, "url can not be blank or null");
        Validate.notEmpty(options.collectionName, "collectionName can not be blank or null");
        Validate.notNull(options.queryCreator, "queryCreator can not be null");
        Validate.notNull(options.mapper, "mapper can not be null");

        this.mongoClient = new MongoDbClient(options.url, options.collectionName);
    }

    public static class Options<T> implements Serializable {
        public String url;
        public String collectionName;
        public MongoMapper mapper;
        public QueryFilterCreator queryCreator;
        public Serializer<T> serializer;
        public int cacheSize = 5000;
        public String globalKey = "$MONGO-MAP-STATE-GLOBAL";
        public String serDocumentField = "tridentSerField";
    }


    @SuppressWarnings("rawtypes")
    public static StateFactory opaque() {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(Options<OpaqueValue> opts) {

        return new Factory(StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional() {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        Options<Object> options = new Options<Object>();
        return nonTransactional(options);
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, opts);
    }


    protected static class Factory implements StateFactory {
        private StateType stateType;
        private Options options;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Factory(StateType stateType, Options options) {
            this.stateType = stateType;
            this.options = options;

            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (this.options.serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            IBackingMap state = new MongoMapState(conf, options);

            if (options.cacheSize > 0) {
                state = new CachedMap(state, options.cacheSize);
            }

            MapState mapState;
            switch (stateType) {
                case NON_TRANSACTIONAL:
                    mapState = NonTransactionalMap.build(state);
                    break;
                case OPAQUE:
                    mapState = OpaqueMap.build(state);
                    break;
                case TRANSACTIONAL:
                    mapState = TransactionalMap.build(state);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown state type: " + stateType);
            }
            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }

    }

    @Override
    public List<T> multiGet(List<List<Object>> keysList) {
        List<T> retval = new ArrayList<>();
        try {
            for (List<Object> keys : keysList) {
                Bson filter = options.queryCreator.createFilterByKeys(keys);
                Document doc = mongoClient.find(filter);
                if (doc != null) {
                    retval.add(this.serializer.deserialize((byte[]) doc.get(options.serDocumentField)));
                } else {
                    retval.add(null);
                }
            }
        } catch (Exception e) {
            LOG.warn("Batch get operation failed.", e);
            throw new FailedException(e);
        }
        return retval;
    }

    @Override
    public void multiPut(List<List<Object>> keysList, List<T> values) {
        try {
            for (int i = 0; i < keysList.size(); i++) {
                List<Object> keys = keysList.get(i);
                T value = values.get(i);
                Bson filter = options.queryCreator.createFilterByKeys(keys);
                Document document = options.mapper.toDocumentByKeys(keys);
                document.append(options.serDocumentField, this.serializer.serialize(value));
                this.mongoClient.update(filter, document, true, false);
            }
        } catch (Exception e) {
            LOG.warn("Batch write operation failed.", e);
            throw new FailedException(e);
        }
    }
}
