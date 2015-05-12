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
package storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class KafkaOffsetScheme implements Scheme {

  public static final String SCHEME_OFFSET_KEY = "offset";
  public static final String SCHEME_PARTITION_KEY = "partition";

  private String _offsetTupleKeyName;
  private String _partitionTupleKeyName;
  private Scheme _localScheme;

  public KafkaOffsetScheme() {
    _localScheme = new StringScheme();
    _offsetTupleKeyName = SCHEME_OFFSET_KEY;
    _partitionTupleKeyName = SCHEME_PARTITION_KEY;
  }


  public KafkaOffsetScheme(Scheme localScheme, String offsetTupleKeyName, String partitionTupleKeyName) {
    _localScheme = localScheme;
    _offsetTupleKeyName = offsetTupleKeyName;
    _partitionTupleKeyName = partitionTupleKeyName;
  }

  public KafkaOffsetScheme(Scheme localScheme) {
    this(localScheme, SCHEME_OFFSET_KEY, SCHEME_PARTITION_KEY);
  }

  public List<Object> deserialize(byte[] bytes) {
    return _localScheme.deserialize(bytes);
  }

  public Fields getOutputFields() {
    List<String> outputFields = _localScheme.getOutputFields().toList();
    outputFields.add(_offsetTupleKeyName);
    outputFields.add(_partitionTupleKeyName);
    return new Fields(outputFields);
  }
}