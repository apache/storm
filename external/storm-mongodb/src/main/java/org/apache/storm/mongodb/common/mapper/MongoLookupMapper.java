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

package org.apache.storm.mongodb.common.mapper;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

public interface MongoLookupMapper extends Serializable {

    /**
     * Converts a Mongo document to a list of storm values that can be emitted. This is done to allow a single
     * storm input tuple and a single Mongo document to result in multiple output values.
     * @param input the input tuple.
     * @param doc the mongo document
     * @return a List of storm values that can be emitted. Each item in list is emitted as an output tuple.
     */
    List<Values> toTuple(ITuple input, Document doc);

    /**
     * declare what are the fields that this code will output.
     * @param declarer OutputFieldsDeclarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
