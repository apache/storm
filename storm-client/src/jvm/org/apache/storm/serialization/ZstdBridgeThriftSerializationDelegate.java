/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.serialization;

import java.util.Map;
import org.apache.storm.utils.Utils;

/**
 * Always writes Zstd out, but tests incoming bytes to determine the format.
 * If Zstd magic is found, it uses {@link ZstdThriftSerializationDelegate}.
 * If not, it falls back to {@link ThriftSerializationDelegate} for raw Thrift.
 */
public class ZstdBridgeThriftSerializationDelegate implements SerializationDelegate {

    private final GzipBridgeThriftSerializationDelegate defaultDelegate = new GzipBridgeThriftSerializationDelegate();
    private final ZstdThriftSerializationDelegate zstdDelegate = new ZstdThriftSerializationDelegate();

    @Override
    public void prepare(Map<String, Object> topoConf) {
        defaultDelegate.prepare(topoConf);
        zstdDelegate.prepare(topoConf);
    }

    @Override
    public byte[] serialize(Object object) {
        // Always compress new data with Zstd
        return zstdDelegate.serialize(object);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if (Utils.ZstdUtils.isZstd(bytes)) {
            return zstdDelegate.deserialize(bytes, clazz);
        } else {
            // Fallback to ZstdBridgeThriftSerializationDelegate
            // it delegates to the proper SerializationDelegate (GzipThriftSerializationDelegate or ThriftSerializationDelegate)
            return defaultDelegate.deserialize(bytes, clazz);
        }
    }
}
