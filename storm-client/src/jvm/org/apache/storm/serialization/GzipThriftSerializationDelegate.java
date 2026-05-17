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

package org.apache.storm.serialization;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

/**
 * Note, this assumes it's deserializing a gzip byte stream, and will err if it encounters any other serialization.
 */
public class GzipThriftSerializationDelegate implements SerializationDelegate {

    private static final int DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024;
    private int maxDecompressedBytes;

    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.maxDecompressedBytes = ObjectReader.getInt(topoConf.getOrDefault(Config.STORM_COMPRESSION_GZIP_MAX_DECOMPRESSED_BYTES,
                DEFAULT_MAX_DECOMPRESSED_BYTES));
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            return Utils.GzipUtils.compress(new TSerializer().serialize((TBase) object));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            TBase instance = (TBase) clazz.newInstance();
            new TDeserializer().deserialize(instance, Utils.GzipUtils.decompress(bytes, this.maxDecompressedBytes));
            return (T) instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
