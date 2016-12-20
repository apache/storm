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
package org.apache.storm.mongodb.trident.state;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.MongoDBClient;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoLookupMapper;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class MongoState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(MongoState.class);

    private Options options;
    private MongoDBClient mongoClient;
    private Map map;

    protected MongoState(Map map, Options options) {
        this.options = options;
        this.map = map;
    }

    public static class Options implements Serializable {
        private String url;
        private String collectionName;
        private MongoMapper mapper;
        private MongoLookupMapper lookupMapper;
        private QueryFilterCreator queryCreator;

        public Options withUrl(String url) {
            this.url = url;
            return this;
        }

        public Options withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Options withMapper(MongoMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Options withMongoLookupMapper(MongoLookupMapper lookupMapper) {
            this.lookupMapper = lookupMapper;
            return this;
        }

        public Options withQueryFilterCreator(QueryFilterCreator queryCreator) {
            this.queryCreator = queryCreator;
            return this;
        }
    }

    protected void prepare() {
        Validate.notEmpty(options.url, "url can not be blank or null");
        Validate.notEmpty(options.collectionName, "collectionName can not be blank or null");

        this.mongoClient = new MongoDBClient(options.url, options.collectionName);
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is noop.");
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        List<Document> documents = Lists.newArrayList();
        for (TridentTuple tuple : tuples) {
            Document document = options.mapper.toDocument(tuple);
            documents.add(document);
        }

        try {
            this.mongoClient.insert(documents, true);
        } catch (Exception e) {
            LOG.warn("Batch write failed but some requests might have succeeded. Triggering replay.", e);
            throw new FailedException(e);
        }
    }

    public List<List<Values>> batchRetrieve(List<TridentTuple> tridentTuples) {
        List<List<Values>> batchRetrieveResult = Lists.newArrayList();
        try {
            for (TridentTuple tuple : tridentTuples) {
                Bson filter = options.queryCreator.createFilter(tuple);
                Document doc = mongoClient.find(filter);
                List<Values> values = options.lookupMapper.toTuple(tuple, doc);
                batchRetrieveResult.add(values);
            }
        } catch (Exception e) {
            LOG.warn("Batch get operation failed. Triggering replay.", e);
            throw new FailedException(e);
        }
        return batchRetrieveResult;
    }
}
