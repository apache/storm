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

package org.apache.storm.sql.runtime.utils;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.storm.spout.Scheme;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.serde.avro.AvroScheme;
import org.apache.storm.sql.runtime.serde.avro.AvroSerializer;
import org.apache.storm.sql.runtime.serde.csv.CsvScheme;
import org.apache.storm.sql.runtime.serde.csv.CsvSerializer;
import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.sql.runtime.serde.tsv.TsvScheme;
import org.apache.storm.sql.runtime.serde.tsv.TsvSerializer;
import org.apache.storm.utils.ReflectionUtils;

public final class SerdeUtils {
    /**
     * Get a Scheme instance based on specific configurations.
     * @param inputFormatClass input format class
     * @param properties Properties
     * @param fieldNames field names
     * @return the Scheme instance
     */
    public static Scheme getScheme(String inputFormatClass, Properties properties, List<String> fieldNames) {
        Scheme scheme;
        if (isNotEmpty(inputFormatClass)) {
            if (JsonScheme.class.getName().equals(inputFormatClass)) {
                scheme = new JsonScheme(fieldNames);
            } else if (TsvScheme.class.getName().equals(inputFormatClass)) {
                String delimiter = properties.getProperty("input.tsv.delimiter", "\t");
                scheme = new TsvScheme(fieldNames, delimiter.charAt(0));
            } else if (CsvScheme.class.getName().equals(inputFormatClass)) {
                scheme = new CsvScheme(fieldNames);
            } else if (AvroScheme.class.getName().equals(inputFormatClass)) {
                String schemaString = properties.getProperty("input.avro.schema");
                Preconditions.checkArgument(isNotEmpty(schemaString), "input.avro.schema can not be empty");
                scheme = new AvroScheme(schemaString, fieldNames);
            } else {
                scheme = ReflectionUtils.newInstance(inputFormatClass);
            }
        } else {
            //use JsonScheme as the default scheme
            scheme = new JsonScheme(fieldNames);
        }
        return scheme;
    }

    /**
     * Get a OutputSerializer instance based on specific configurations.
     * @param outputFormatClass output format class
     * @param properties Properties
     * @param fieldNames field names
     * @return the OutputSerializer instance
     */
    public static IOutputSerializer getSerializer(String outputFormatClass, Properties properties, List<String> fieldNames) {
        IOutputSerializer serializer;
        if (isNotEmpty(outputFormatClass)) {
            if (JsonSerializer.class.getName().equals(outputFormatClass)) {
                serializer = new JsonSerializer(fieldNames);
            } else if (TsvSerializer.class.getName().equals(outputFormatClass)) {
                String delimiter = properties.getProperty("output.tsv.delimiter", "\t");
                serializer = new TsvSerializer(fieldNames, delimiter.charAt(0));
            } else if (CsvSerializer.class.getName().equals(outputFormatClass)) {
                serializer = new CsvSerializer(fieldNames);
            } else if (AvroSerializer.class.getName().equals(outputFormatClass)) {
                String schemaString = properties.getProperty("output.avro.schema");
                Preconditions.checkArgument(isNotEmpty(schemaString), "output.avro.schema can not be empty");
                serializer = new AvroSerializer(schemaString, fieldNames);
            } else {
                serializer = ReflectionUtils.newInstance(outputFormatClass);
            }
        } else {
            //use JsonSerializer as the default serializer
            serializer = new JsonSerializer(fieldNames);
        }
        return serializer;
    }

    /**
     * Convert a Avro object to a Java object, changing the Avro Utf8 type to Java String.
     * @param value Avro object
     * @return Java object
     */
    public static Object convertAvroUtf8(Object value) {
        Object ret;
        if (value instanceof Utf8) {
            ret = value.toString();
        } else if (value instanceof Map<?, ?>) {
            ret = convertAvroUtf8Map((Map<Object, Object>) value);
        } else if (value instanceof GenericData.Array) {
            ret = convertAvroUtf8Array((GenericData.Array) value);
        } else {
            ret = value;
        }
        return ret;
    }

    private static Object convertAvroUtf8Map(Map<Object, Object> value) {
        Map<Object, Object> map = new HashMap<>(value.size());
        for (Map.Entry<Object, Object> entry : value.entrySet()) {
            Object k = convertAvroUtf8(entry.getKey());
            Object v = convertAvroUtf8(entry.getValue());
            map.put(k, v);
        }
        return map;
    }

    private static Object convertAvroUtf8Array(GenericData.Array value) {
        List<Object> ls = new ArrayList<>(value.size());
        for (Object o : value) {
            ls.add(convertAvroUtf8(o));
        }
        return ls;
    }
}
