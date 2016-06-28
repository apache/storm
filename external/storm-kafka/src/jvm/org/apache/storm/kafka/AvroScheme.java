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
package org.apache.storm.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.storm.Config;
import org.apache.storm.avro.DefaultDirectAvroSerializer;
import org.apache.storm.avro.DirectAvroSerializer;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class AvroScheme implements Scheme {
    public static final String AVRO_SCHEME_KEY = "avro";
    DirectAvroSerializer serializer = new DefaultDirectAvroSerializer();
    Schema schema;

    /**
     * @param schemaString json format schema string
     */
    public AvroScheme(String schemaString) {
        schema = new Schema.Parser().parse(schemaString);
    }

    /**
     * for confluent avro schema registry
     * 
     * @param url schema registry server url
     * @param id schema id
     */
    public AvroScheme(String url, int id) {
        getRegistrySchema(url, id);
    }

    /**
     * @param id schema id
     * @param stormConf storm configuration
     */
    public AvroScheme(int id, Map stormConf) {
        String url = (String) stormConf.get(Config.TOPOLOGY_AVRO_CONFLUENT_SCHEMA_REGISTRY_URL);
        getRegistrySchema(url, id);
    }

    private void getRegistrySchema(String url, int id) {
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 10000);
        try {
            schema = client.getByID(id);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            GenericContainer record = null;
            if(byteBuffer.hasArray()) {
                record = serializer.deserialize(byteBuffer.array(), schema);
            } else {
                record = serializer.deserialize(Utils.toByteArray(byteBuffer), schema);
            }
            return new Values(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields(AVRO_SCHEME_KEY);
    }
}
