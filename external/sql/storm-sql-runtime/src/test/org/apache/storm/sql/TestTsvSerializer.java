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
import org.apache.storm.sql.runtime.serde.tsv.TsvScheme;
import org.apache.storm.sql.runtime.serde.tsv.TsvSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TestTsvSerializer {

  @Test
  public void testTsvSchemeAndSerializer() {
    final char delimiter = '\t';

    List<String> fields = Lists.newArrayList("ID", "val");
    List<Object> o = Lists.newArrayList("1", "2");

    TsvSerializer serializer = new TsvSerializer(fields, delimiter);
    ByteBuffer byteBuffer = serializer.write(o, null);

    TsvScheme scheme = new TsvScheme(fields, delimiter);
    assertArrayEquals(o.toArray(), scheme.deserialize(byteBuffer).toArray());
  }

}
