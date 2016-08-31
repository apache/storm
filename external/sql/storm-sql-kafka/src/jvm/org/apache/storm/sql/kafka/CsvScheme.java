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
package org.apache.storm.sql.kafka;

import org.apache.storm.spout.Scheme;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.tuple.Fields;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CsvScheme implements Scheme {
  List<FieldInfo> fieldsInfo = null;
  List<String> fields = null;

  public CsvScheme(List<FieldInfo> fieldsInfo) {
    this.fieldsInfo = fieldsInfo;
    fields = new ArrayList<>();
    for (FieldInfo info : fieldsInfo) {
      fields.add(info.name());
    }
  }

  @Override
  public List<Object> deserialize(ByteBuffer ser) {
    String str = ser.toString();
    String[] parts = str.split(",");
    if (parts.length != this.fields.size()) {
      throw new RuntimeException("Kafka CSV message does not match the designed schema");
    }
    List<Object> ret = new ArrayList<>(this.fields.size());
    for (int i = 0; i < this.fieldsInfo.size(); i++) {
      FieldInfo fieldInfo = this.fieldsInfo.get(i);
      String part = parts[i];
      Class<?> type = fieldInfo.type();
      if (type == String.class) {
        ret.add(part);
      } else {
        try {
          Constructor cons = type.getDeclaredConstructor(String.class);
          ret.add(cons.newInstance(part));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return ret;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(this.fields);
  }
}
