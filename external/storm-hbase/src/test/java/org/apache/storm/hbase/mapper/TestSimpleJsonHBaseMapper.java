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
package org.apache.storm.hbase.mapper;

import org.apache.storm.Config;
import org.apache.storm.hbase.bolt.mapper.SimpleJsonHBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.Assert;
import java.util.HashMap;

public class TestSimpleJsonHBaseMapper {

  @Test
  public void testSimpleJsonHBaseMapper() throws ParseException {

    String data = "{\"rowkey\":\"1234\",\"latitude\":\"35.4535201\",\"alt\":\"108700\"\"user.name\":\"tom\"}";
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("rowkey", "1234");
    jsonObject.put("latitude", "35.4535201");
    jsonObject.put("alt", "108700");
    jsonObject.put("user.name", "tom");

    Tuple tuple = generateTestTuple(data);

    SimpleJsonHBaseMapper mapper = new SimpleJsonHBaseMapper().withRowKeyField("rowkey").useAllAvailableKeys()
            .withColumnFamily("f1");
    Assert.assertEquals(jsonObject.get("rowkey"), new String(mapper.rowKey(tuple)));
    ColumnList columnList = mapper.columns(tuple);
    Assert.assertEquals(jsonObject.size() - 1, columnList.getColumns().size());
    for (ColumnList.Column col : columnList.getColumns()) {
       Assert.assertEquals(new String(col.getValue()), jsonObject.get(new String(col.getQualifier())));
    }

    Fields fields = new Fields("latitude");
    SimpleJsonHBaseMapper mapper_limited = new SimpleJsonHBaseMapper().withRowKeyField("rowkey")
            .withColumnFields(fields);
    ColumnList columnList1 = mapper_limited.columns((tuple));
    Assert.assertEquals(1, columnList1.getColumns().size());
    ColumnList.Column col1 = columnList1.getColumns().get(0);
    Assert.assertEquals(jsonObject.get("latitude"), new String(col1.getValue()));
  }

  private Tuple generateTestTuple(Object data) {
    TopologyBuilder builder = new TopologyBuilder();
    GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
            new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
      @Override
      public Fields getComponentOutputFields(String componentId, String streamId) {
        return new Fields("data");
      }
    };
    return new TupleImpl(topologyContext, new Values(data), 1, "");
  }
}