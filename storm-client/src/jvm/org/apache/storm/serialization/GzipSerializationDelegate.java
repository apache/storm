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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.io.input.BoundedInputStream;
import org.apache.storm.utils.ObjectReader;

/**
 * Note, this assumes it's deserializing a gzip byte stream, and will err if it encounters any other serialization.
 */
public class GzipSerializationDelegate implements SerializationDelegate {

    private static final int DEFAULT_MAX_DECOMPRESSED_BYTES = 10 * 1024 * 1024;
    private int maxDecompressedBytes;

    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.maxDecompressedBytes = ObjectReader.getInt(topoConf.getOrDefault(Config.STORM_COMPRESSION_GZIP_MAX_DECOMPRESSED_BYTES,
                DEFAULT_MAX_DECOMPRESSED_BYTES));
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gos = new GZIPOutputStream(bos);
            ObjectOutputStream oos = new ObjectOutputStream(gos);
            oos.writeObject(object);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             GZIPInputStream gis = new GZIPInputStream(bis);
             BoundedInputStream lis = BoundedInputStream.builder()
                     .setMaxCount(this.maxDecompressedBytes)
                     .setInputStream(gis)
                     .setPropagateClose(true)
                     .get();
             ObjectInputStream ois = new ObjectInputStream(lis)) {
            Object ret = ois.readObject();
            if (gis.read() != -1) {
                throw new IOException("Decompression threshold exceeded! Possible security risk or invalid data size.");
            }
            return clazz.cast(ret);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed: " + e.getMessage(), e);
        }
    }
}
