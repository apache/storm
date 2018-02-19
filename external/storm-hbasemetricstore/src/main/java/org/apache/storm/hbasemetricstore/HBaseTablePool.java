/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hbasemetricstore;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.storm.metricstore.MetricException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object pool for HTableInterface to allow reuse of HBase Table objects.
 */
public class HBaseTablePool extends BasePooledObjectFactory<Table> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTablePool.class);
    private static HBaseTablePool instance;
    private GenericObjectPool<Table> pool;
    private Configuration conf;
    private String namespace;
    private String tableName;

    private HBaseTablePool(Configuration conf, String namespace, String tableName) throws MetricException {
        pool = new GenericObjectPool<>(this);
        pool.setBlockWhenExhausted(false);
        pool.setMaxTotal(-1); // No limit
        this.conf = conf;
        this.namespace = namespace;
        this.tableName = tableName;
    }

    static void init(Configuration conf, String namespace, String tableName) throws MetricException {
        if (instance != null) {
            throw new MetricException("HBase Table pool already exists");
        }
        instance = new HBaseTablePool(conf, namespace, tableName);
    }

    static Table allocate() throws MetricException {
        try {
            return instance.pool.borrowObject();
        } catch (Exception e) {
            String message = "Failed to allocate HBase table interface";
            LOG.error(message, e);
            throw new MetricException(message, e);
        }
    }

    static void release(Table table) {
        if (table == null) {
            return;
        }
        instance.pool.returnObject(table);
    }

    static void shutdown()  {
        instance.close();
    }

    @Override
    public Table create() throws Exception {
        TableName metricsTableName = TableName.valueOf(this.namespace, this.tableName);
        Connection connection = ConnectionFactory.createConnection(this.conf);
        Table metricsTable = connection.getTable(metricsTableName);
        if (metricsTable == null) {
            String message = "Failed to get metrics table";
            LOG.error(message);
            throw new MetricException(message);
        }
        return metricsTable;
    }

    @Override
    public PooledObject<Table> wrap(Table htableInterface) {
        return new DefaultPooledObject<>(htableInterface);
    }

    @Override
    public void destroyObject(PooledObject<Table> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }

    @Override
    public void close() {
        this.pool.close();
    }
}


