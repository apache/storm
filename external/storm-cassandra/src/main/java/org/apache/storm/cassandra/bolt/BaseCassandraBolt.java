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

package org.apache.storm.cassandra.bolt;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.cassandra.BaseExecutionResultHandler;
import org.apache.storm.cassandra.CassandraContext;
import org.apache.storm.cassandra.ExecutionResultHandler;
import org.apache.storm.cassandra.client.CassandraConf;
import org.apache.storm.cassandra.client.SimpleClient;
import org.apache.storm.cassandra.client.SimpleClientProvider;
import org.apache.storm.cassandra.executor.AsyncExecutor;
import org.apache.storm.cassandra.executor.AsyncExecutorProvider;
import org.apache.storm.cassandra.executor.AsyncResultHandler;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base cassandra bolt.
 *
 * <p>Default {@link org.apache.storm.topology.base.BaseTickTupleAwareRichBolt}
 */
public abstract class BaseCassandraBolt<T> extends BaseTickTupleAwareRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraBolt.class);

    protected OutputCollector outputCollector;

    protected SimpleClientProvider clientProvider;
    protected SimpleClient client;
    protected Session session;
    protected Map<String, Object> topoConfig;

    protected CassandraConf cassandraConf;

    private CQLStatementTupleMapper mapper;
    private ExecutionResultHandler resultHandler;

    private transient Map<String, Fields> outputsFields = new HashMap<>();
    private Map<String, Object> cassandraConfig;

    /**
     * Creates a new {@link CassandraWriterBolt} instance.
     */
    public BaseCassandraBolt(CQLStatementTupleMapper mapper, SimpleClientProvider clientProvider) {
        this.mapper = mapper;
        this.clientProvider = clientProvider;
    }

    /**
     * Creates a new {@link CassandraWriterBolt} instance.
     */
    public BaseCassandraBolt(CQLStatementTupleMapper tupleMapper) {
        this(tupleMapper, new CassandraContext());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map<String, Object> topoConfig, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topoConfig = topoConfig;

        Map<String, Object> cassandraClientConfig = cassandraConfig != null ? cassandraConfig : topoConfig;

        this.cassandraConf = new CassandraConf(cassandraClientConfig);
        this.client = clientProvider.getClient(cassandraClientConfig);

        try {
            session = client.connect();
        } catch (NoHostAvailableException e) {
            outputCollector.reportError(e);
            throw e;
        }
    }

    public BaseCassandraBolt withResultHandler(ExecutionResultHandler resultHandler) {
        this.resultHandler = resultHandler;
        return this;
    }

    /**
     * Configures this bolt with the given {@code fields} as outputfields with stream id as {@link Utils#DEFAULT_STREAM_ID}.
     *
     * @param fields outputfields
     */
    public BaseCassandraBolt withOutputFields(Fields fields) {
        Preconditions.checkNotNull(fields, "fields should not be null.");
        this.outputsFields.put(Utils.DEFAULT_STREAM_ID, fields);
        return this;
    }

    /**
     * Configures this bolt given {@code fields} as outputfields for the given {@code stream}.
     */
    public BaseCassandraBolt withStreamOutputFields(String stream, Fields fields) {
        if (stream == null || stream.length() == 0) {
            throw new IllegalArgumentException("'stream' should not be null");
        }
        this.outputsFields.put(stream, fields);
        return this;
    }

    /**
     * Takes the given {@code config} for creating cassandra client.
     * {@link CassandraConf} contains all the properties that can be configured.
     */
    public BaseCassandraBolt withCassandraConfig(Map<String, Object> config) {
        if (config == null) {
            throw new IllegalArgumentException("config should not be null");
        }
        cassandraConfig = config;
        return this;
    }

    protected ExecutionResultHandler getResultHandler() {
        if (resultHandler == null) {
            resultHandler = new BaseExecutionResultHandler();
        }
        return resultHandler;
    }

    protected CQLStatementTupleMapper getMapper() {
        return mapper;
    }

    protected abstract AsyncResultHandler<T> getAsyncHandler();

    protected AsyncExecutor<T> getAsyncExecutor() {
        return AsyncExecutorProvider.getLocal(session, getAsyncHandler());
    }

    /**
     * {@inheritDoc}
     *
     * @param input the tuple to process.
     */
    @Override
    public final void execute(Tuple input) {
        getAsyncHandler().flush(outputCollector);
        super.execute(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // outputsFields can be empty if this bolt acts like a sink in topology.
        if (!outputsFields.isEmpty()) {
            Fields fields = outputsFields.remove(Utils.DEFAULT_STREAM_ID);
            if (fields != null) {
                declarer.declare(fields);
            }
            for (Map.Entry<String, Fields> entry : outputsFields.entrySet()) {
                declarer.declareStream(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        // add tick tuple each second to force acknowledgement of pending tuples.
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        getAsyncExecutor().shutdown();
        getAsyncHandler().flush(outputCollector);
        client.close();
    }
}
