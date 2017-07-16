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

package org.apache.storm.sql.runtime.serde.csv;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * CsvScheme uses the standard RFC4180 CSV Parser
 * One of the difference from Tsv format is that fields with embedded commas will be quoted.
 * eg: a,"b,c",d is allowed.
 *
 * @see <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
 */
public class CsvScheme implements Scheme {
    private final List<String> fieldNames;

    public CsvScheme(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        try {
            String data = new String(Utils.toByteArray(ser), StandardCharsets.UTF_8);
            CSVParser parser = CSVParser.parse(data, CSVFormat.RFC4180);
            CSVRecord record = parser.getRecords().get(0);
            Preconditions.checkArgument(record.size() == fieldNames.size(), "Invalid schema");

            ArrayList<Object> list = new ArrayList<>(fieldNames.size());
            for (int i = 0; i < record.size(); i++) {
                list.add(record.get(i));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldNames);
    }
}
