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

package org.apache.storm.cassandra.query.builder;

import com.datastax.driver.core.TypeCodec;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.impl.ObjectMapperCqlStatementMapper;
import org.apache.storm.lambda.SerializableCallable;

/**
 * Builds a tuple mapper that is able to map objects annotated with {@link com.datastax.driver.mapping.annotations.Table} to CQL statements.
 *
 * <p>
 *     Needs at least <pre>operationField</pre> and <pre>valueField</pre>.
 *     Writetime, TTL and consistency level can be specified in optional tuple fields.
 * </p>
 */
public class ObjectMapperCqlStatementMapperBuilder implements CQLStatementBuilder<ObjectMapperCqlStatementMapper>, Serializable {
    private final String operationField;
    private final String valueField;
    private final Collection<Class<?>> udtClasses = new ArrayList<>();
    private final Collection<SerializableCallable<TypeCodec<?>>> codecProducers = new ArrayList<>();
    private String timestampField;
    private String ttlField;
    private String consistencyLevelField;

    public ObjectMapperCqlStatementMapperBuilder(String operationField, String valueField) {
        this.operationField = operationField;
        this.valueField = valueField;
    }

    /**
     * Builds an ObjectMapperCqlStatementMapper.
     */
    @Override
    public ObjectMapperCqlStatementMapper build() {
        List<TypeCodec<?>> codecs = codecProducers.stream().map(codecProducer -> {
            try {
                return codecProducer.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        return new ObjectMapperCqlStatementMapper(operationField, valueField, timestampField, ttlField, consistencyLevelField,
                                                  codecs, udtClasses);
    }

    public ObjectMapperCqlStatementMapperBuilder withCodecs(List<SerializableCallable<TypeCodec<?>>> codecProducer) {
        this.codecProducers.addAll(codecProducer);
        return this;
    }

    public ObjectMapperCqlStatementMapperBuilder withUdtCodecs(List<Class<?>> udtClass) {
        this.udtClasses.addAll(udtClass);
        return this;
    }

    public ObjectMapperCqlStatementMapperBuilder withTimestampField(String timestampField) {
        this.timestampField = timestampField;
        return this;
    }

    public ObjectMapperCqlStatementMapperBuilder withTtlField(String ttlField) {
        this.ttlField = ttlField;
        return this;
    }

    public ObjectMapperCqlStatementMapperBuilder withConsistencyLevelField(String consistencyLevelField) {
        this.consistencyLevelField = consistencyLevelField;
        return this;
    }
}

