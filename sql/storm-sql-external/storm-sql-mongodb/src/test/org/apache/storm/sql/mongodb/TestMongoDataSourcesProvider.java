/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.mongodb;

import java.net.URI;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.topology.IRichBolt;
import org.junit.Assert;
import org.junit.Test;

public class TestMongoDataSourcesProvider {

    private static final List<FieldInfo> FIELDS = ImmutableList.of(
        new FieldInfo("ID", int.class, true),
        new FieldInfo("val", String.class, false));
    private static final Properties TBL_PROPERTIES = new Properties();

    static {
        TBL_PROPERTIES.put("collection.name", "collection1");
        TBL_PROPERTIES.put("ser.field", "serField");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMongoSink() throws Exception {
        ISqlStreamsDataSource ds = DataSourcesRegistry.constructStreamsDataSource(
            URI.create("mongodb://127.0.0.1:27017/test"), null, null, TBL_PROPERTIES, FIELDS);
        Assert.assertNotNull(ds);

        IRichBolt consumer = ds.getConsumer();
        Assert.assertEquals(MongoInsertBolt.class, consumer.getClass());
    }
}
