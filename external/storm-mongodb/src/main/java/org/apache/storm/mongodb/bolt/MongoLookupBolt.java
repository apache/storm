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

package org.apache.storm.mongodb.bolt;

import java.util.List;
import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Basic bolt for querying from MongoDB.
 * Note: Each MongoLookupBolt defined in a topology is tied to a specific collection.
 */
public class MongoLookupBolt extends AbstractMongoBolt {

    private QueryFilterCreator queryCreator;
    private MongoLookupMapper mapper;

    /**
     * MongoLookupBolt Constructor.
     * @param url The MongoDB server url
     * @param collectionName The collection where reading/writing data
     * @param queryCreator QueryFilterCreator
     * @param mapper MongoMapper converting tuple to an MongoDB document
     */
    public MongoLookupBolt(String url, String collectionName, QueryFilterCreator queryCreator, MongoLookupMapper mapper) {
        super(url, collectionName);

        Validate.notNull(queryCreator, "QueryFilterCreator can not be null");
        Validate.notNull(mapper, "MongoLookupMapper can not be null");

        this.queryCreator = queryCreator;
        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            return;
        }

        try {
            //get query filter
            Bson filter = queryCreator.createFilter(tuple);
            //find document from mongodb
            Document doc = mongoClient.find(filter);
            //get storm values and emit
            List<Values> valuesList = mapper.toTuple(tuple, doc);
            for (Values values : valuesList) {
                this.collector.emit(tuple, values);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        mapper.declareOutputFields(declarer);
    }

}
