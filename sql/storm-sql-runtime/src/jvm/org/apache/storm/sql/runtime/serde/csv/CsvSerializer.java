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

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.storm.sql.runtime.IOutputSerializer;

/**
 * CsvSerializer uses the standard RFC4180 CSV Parser
 * One of the difference from Tsv format is that fields with embedded commas will be quoted.
 * eg: a,"b,c",d is allowed.
 *
 * @see <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
 */
public class CsvSerializer implements IOutputSerializer, Serializable {
    private final List<String> fields; //reserved for future

    public CsvSerializer(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public ByteBuffer write(List<Object> data, ByteBuffer buffer) {
        try {
            StringWriter writer = new StringWriter();
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180);
            for (Object o : data) {
                printer.print(o);
            }
            //since using StringWriter, we do not need to close it.
            return ByteBuffer.wrap(writer.getBuffer().toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
