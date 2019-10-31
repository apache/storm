/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.query.impl;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.ObjectMapperOperation;
import org.apache.storm.tuple.ITuple;

/**
 * Tuple mapper that is able to map objects annotated with {@link com.datastax.driver.mapping.annotations.Table} to CQL statements.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ObjectMapperCqlStatementMapper implements CQLStatementTupleMapper {
    private static final Map<Session, MappingManager> mappingManagers = new WeakHashMap<>();

    private final String operationField;
    private final String valueField;
    private final String timestampField;
    private final String ttlField;
    private final String consistencyLevelField;
    private final Collection<TypeCodec<?>> codecs;
    private final Collection<Class<?>> udtClasses;

    public ObjectMapperCqlStatementMapper(String operationField, String valueField, String timestampField, String ttlField,
                                          String consistencyLevelField, Collection<TypeCodec<?>> codecs, Collection<Class<?>> udtClasses) {
        Preconditions.checkNotNull(operationField, "Operation field must not be null");
        Preconditions.checkNotNull(valueField, "Value field should not be null");
        this.operationField = operationField;
        this.valueField = valueField;
        this.timestampField = timestampField;
        this.ttlField = ttlField;
        this.consistencyLevelField = consistencyLevelField;
        this.codecs = codecs;
        this.udtClasses = udtClasses;
    }

    @Override
    public List<Statement> map(Map<String, Object> map, Session session, ITuple tuple) {
        final ObjectMapperOperation operation = (ObjectMapperOperation) tuple.getValueByField(operationField);

        Preconditions.checkNotNull(operation, "Operation must not be null");

        final Object value = tuple.getValueByField(valueField);
        final Object timestampObject = timestampField != null ? tuple.getValueByField(timestampField) : null;
        final Object ttlObject = ttlField != null ? tuple.getValueByField(ttlField) : null;
        final ConsistencyLevel consistencyLevel =
            consistencyLevelField != null ? (ConsistencyLevel) tuple.getValueByField(consistencyLevelField) : null;

        final Class<?> valueClass = value.getClass();

        final Mapper mapper = getMappingManager(session).mapper(valueClass);

        Collection<Option> options = new ArrayList<>();

        if (timestampObject != null) {
            if (timestampObject instanceof Number) {
                options.add(Option.timestamp(((Number) timestampObject).longValue()));
            } else if (timestampObject instanceof Instant) {
                Instant timestamp = (Instant) timestampObject;
                options.add(Option.timestamp(timestamp.getEpochSecond() * 1000_0000L + timestamp.getNano() / 1000L));
            }
        }

        if (ttlObject != null) {
            if (ttlObject instanceof Number) {
                options.add(Option.ttl(((Number) ttlObject).intValue()));
            } else if (ttlObject instanceof Duration) {
                Duration ttl = (Duration) ttlObject;
                options.add(Option.ttl((int) ttl.getSeconds()));
            }
        }

        if (consistencyLevel != null) {
            options.add(Option.consistencyLevel(consistencyLevel));
        }

        if (operation == ObjectMapperOperation.SAVE) {
            options.add(Option.saveNullFields(true));
            return Arrays.asList(mapper.saveQuery(value, options.toArray(new Option[options.size()])));
        } else if (operation == ObjectMapperOperation.SAVE_IGNORE_NULLS) {
            options.add(Option.saveNullFields(false));
            return Arrays.asList(mapper.saveQuery(value, options.toArray(new Option[options.size()])));
        } else if (operation == ObjectMapperOperation.DELETE) {
            return Arrays.asList(mapper.deleteQuery(value, options.toArray(new Option[options.size()])));
        } else {
            throw new UnsupportedOperationException("Unknown operation: " + operation);
        }
    }

    private MappingManager getMappingManager(Session session) {
        synchronized (mappingManagers) {
            MappingManager mappingManager = mappingManagers.get(session);
            if (mappingManager == null) {
                mappingManager = new MappingManager(session);
                mappingManagers.put(session, mappingManager);
                CodecRegistry codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();
                for (TypeCodec<?> codec : codecs) {
                    codecRegistry.register(codec);
                }
                for (Class<?> udtClass : udtClasses) {
                    mappingManager.udtCodec(udtClass);
                }
            }
            return mappingManager;
        }
    }
}
