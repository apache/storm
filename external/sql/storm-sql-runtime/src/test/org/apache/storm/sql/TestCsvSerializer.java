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
package org.apache.storm.sql;

import com.google.common.collect.Lists;
import org.apache.storm.sql.runtime.serde.csv.CsvScheme;
import org.apache.storm.sql.runtime.serde.csv.CsvSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TestCsvSerializer {

  @Test
  public void testCsvSchemeAndSerializer() {
    List<String> fields = Lists.newArrayList("ID", "val");
    List<Object> o = Lists.newArrayList("1", "2");

    CsvSerializer serializer = new CsvSerializer(fields);
    ByteBuffer byteBuffer = serializer.write(o, null);

    CsvScheme scheme = new CsvScheme(fields);
    assertArrayEquals(o.toArray(), scheme.deserialize(byteBuffer).toArray());

    // Fields with embedded commas or double-quote characters
    fields = Lists.newArrayList("ID", "val", "v");
    o = Lists.newArrayList("1,9", "2,\"3\",5", "\"8\"");

    serializer = new CsvSerializer(fields);
    byteBuffer = serializer.write(o, null);

    scheme = new CsvScheme(fields);
    assertArrayEquals(o.toArray(), scheme.deserialize(byteBuffer).toArray());
  }

}
