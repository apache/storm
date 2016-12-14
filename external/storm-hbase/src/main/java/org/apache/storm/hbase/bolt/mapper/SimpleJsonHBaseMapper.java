/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hbase.bolt.mapper;

import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.hbase.common.Utils.*;

public class SimpleJsonHBaseMapper implements HBaseMapper {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleJsonHBaseMapper.class);

  private String rowKeyField;
  private byte[] columnFamily;
  private Fields columnFields;
  private Boolean useAllAvailableKeys = Boolean.FALSE;

  private static final JSONParser parser = new JSONParser();

  public SimpleJsonHBaseMapper() {
  }

  public SimpleJsonHBaseMapper withRowKeyField(String rowKeyField) {
    this.rowKeyField = rowKeyField;
    return this;
  }

  public SimpleJsonHBaseMapper withColumnFields(Fields columnFields) {
    this.columnFields = columnFields;
    return this;
  }

  public SimpleJsonHBaseMapper useAllAvailableKeys() {
    this.useAllAvailableKeys = Boolean.TRUE;
    return this;
  }

  public SimpleJsonHBaseMapper withColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily.getBytes();
    return this;
  }

  public byte[] rowKey(Tuple tuple) throws ParseException {
    JSONObject jObject = (JSONObject) parser.parse(tuple.getString(0));
    return toBytes(jObject.get(this.rowKeyField));
  }

  public ColumnList columns(Tuple tuple) throws ParseException {
    JSONObject jColumns = (JSONObject) parser.parse(tuple.getString(0));

    ColumnList cols = new ColumnList();
    if (useAllAvailableKeys) {
      for (Object key : jColumns.keySet()) {
        if (key.hashCode() != this.rowKeyField.hashCode()) {
          cols.addColumn(this.columnFamily, key.toString().getBytes(), toBytes(jColumns.get(key)));
        }
      }
    }
      else {
      if (this.columnFields != null) {
        for (String field : this.columnFields) {
          if (field != this.rowKeyField) {
            cols.addColumn(this.columnFamily, field.getBytes(), toBytes(jColumns.get(field)));
          }
        }
      }
    }
    return cols;
  }
}
