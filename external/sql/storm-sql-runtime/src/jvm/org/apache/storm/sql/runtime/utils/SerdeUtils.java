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
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Properties;

public final class SerdeUtils {
    public static Scheme getScheme(String inputFormatClass, Properties properties, List<String> fieldNames) {
        Scheme scheme;
        if (isNotEmpty(inputFormatClass)) {
            switch (inputFormatClass) {
                case "org.apache.storm.sql.runtime.serde.json.JsonScheme" :
                    scheme = new JsonScheme(fieldNames);
                    break;
                case "org.apache.storm.sql.runtime.serde.json.TsvScheme" :
                    String delimiter = properties.getProperty("tsv.delimiter", "\t");
                    scheme = new TsvScheme(fieldNames, delimiter.charAt(0));
                    break;
                case "org.apache.storm.sql.runtime.serde.json.CsvScheme" :
                    scheme = new CsvScheme(fieldNames);
                    break;
                case "org.apache.storm.sql.runtime.serde.avro.AvroScheme" :
                    String schemaString = properties.getProperty("avro.schema");
                    Preconditions.checkArgument(isNotEmpty(schemaString), "avro.schema can not be empty");
                    scheme = new AvroScheme(schemaString, fieldNames);
                    break;
                default:
                    //TODO we can use type.getDeclaredConstructor(List.class).newInstance(fieldNames)
                    //and replace above known XXXScheme(fieldNames)
                    //I will update this after previous PRs getting merged
                    scheme = Utils.newInstance(inputFormatClass);
            }
        } else {
            //use JsonScheme as the default scheme
            scheme = new JsonScheme(fieldNames);
        }
        return scheme;
    }

    public static IOutputSerializer getSerializer(String outputFormatClass, Properties properties, List<String> fieldNames) {
        IOutputSerializer serializer;
        if (isNotEmpty(outputFormatClass)) {
            switch (outputFormatClass) {
                case "org.apache.storm.sql.runtime.serde.json.JsonSerializer" :
                    serializer = new JsonSerializer(fieldNames);
                    break;
                case "org.apache.storm.sql.runtime.serde.json.TsvSerializer" :
                    String delimiter = properties.getProperty("tsv.delimiter", "\t");
                    serializer = new TsvSerializer(fieldNames, delimiter.charAt(0));
                    break;
                case "org.apache.storm.sql.runtime.serde.json.CsvSerializer" :
                    serializer = new CsvSerializer(fieldNames);
                    break;
                case "org.apache.storm.sql.runtime.serde.avro.AvroSerializer" :
                    String schemaString = properties.getProperty("avro.schema");
                    Preconditions.checkArgument(isNotEmpty(schemaString), "avro.schema can not be empty");
                    serializer = new AvroSerializer(schemaString, fieldNames);
                    break;
                default:
                    serializer = Utils.newInstance(outputFormatClass);
            }
        } else {
            //use JsonSerializer as the default serializer
            serializer = new JsonSerializer(fieldNames);
        }
        return serializer;
    }
}
