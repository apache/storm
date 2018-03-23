#Storm MongoDB

Storm/Trident integration for [MongoDB](https://www.mongodb.org/). This package includes the core bolts and trident states that allows a storm topology to either insert storm tuples in a database collection or to execute select/update queries against a database collection in a storm topology.

## Insert into Database
The bolt and trident state included in this package for inserting data into a database collection.

### MongoMapper
The main API for inserting data in a collection using MongoDB is the `org.apache.storm.mongodb.common.mapper.MongoMapper` interface:

```java
public interface MongoMapper extends Serializable {
    Document toDocument(ITuple tuple);
    Document toDocumentByKeys(List<Object> keys);
}
```

### SimpleMongoMapper
`storm-mongodb` includes a general purpose `MongoMapper` implementation called `SimpleMongoMapper` that can map Storm tuple to a Database document.  `SimpleMongoMapper` assumes that the storm tuple has fields with same name as the document field name in the database collection that you intend to write to.

```java
public class SimpleMongoMapper implements MongoMapper {
    private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        return document;
    }

    @Override
    public Document toDocumentByKeys(List<Object> keys) {
        Document document = new Document();
        document.append("_id", MongoUtils.getID(keys));
        return document;
    }

    public SimpleMongoMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}
```

### MongoInsertBolt
To use the `MongoInsertBolt`, you construct an instance of it by specifying url, collectionName and a `MongoMapper` implementation that converts storm tuple to DB document. The following is the standard URI connection scheme:
 `mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]`

More options information(eg: Write Concern Options) about Mongo URI, you can visit https://docs.mongodb.org/manual/reference/connection-string/#connections-connection-options

 ```java
String url = "mongodb://127.0.0.1:27017/test";
String collectionName = "wordcount";

MongoMapper mapper = new SimpleMongoMapper()
        .withFields("word", "count");

MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName, mapper);
 ```


## Update from Database
The bolt included in this package for updating data from a database collection.

### MongoUpdateMapper
The main API for updating data in a collection using MongoDB is the `org.apache.storm.mongodb.common.mapper.MongoUpdateMapper` interface:

```java
public interface MongoUpdateMapper extends MongoMapper { }
```

### SimpleMongoUpdateMapper
`storm-mongodb` includes a general purpose `MongoUpdateMapper` implementation called `SimpleMongoUpdateMapper` that can map Storm tuple to a Database document. `SimpleMongoUpdateMapper` assumes that the storm tuple has fields with same name as the document field name in the database collection that you intend to write to.
`SimpleMongoUpdateMapper` uses `$set` operator for setting the value of a field in a document. More information about update operator, you can visit 
https://docs.mongodb.org/manual/reference/operator/update/

```java
public class SimpleMongoUpdateMapper extends SimpleMongoMapper implements MongoUpdateMapper {

    private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        //$set operator: Sets the value of a field in a document.
        return new Document("$set", document);
    }

    public SimpleMongoUpdateMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}
```


### QueryFilterCreator
The main API for creating a MongoDB query Filter is the `org.apache.storm.mongodb.common.QueryFilterCreator` interface:

 ```java
public interface QueryFilterCreator extends Serializable {
    Bson createFilter(ITuple tuple);
    Bson createFilterByKeys(List<Object> keys);
}
 ```

### SimpleQueryFilterCreator
`storm-mongodb` includes a general purpose `QueryFilterCreator` implementation called `SimpleQueryFilterCreator` that can create a MongoDB query Filter by given Tuple.  `QueryFilterCreator` uses `$eq` operator for matching values that are equal to a specified value. More information about query operator, you can visit 
https://docs.mongodb.org/manual/reference/operator/query/

 ```java
public class SimpleQueryFilterCreator implements QueryFilterCreator {

    private String field;
    
    @Override
    public Bson createFilter(ITuple tuple) {
        return Filters.eq(field, tuple.getValueByField(field));
    }

    @Override
    public Bson createFilterByKeys(List<Object> keys) {
        return Filters.eq("_id", MongoUtils.getID(keys));
    }

    public SimpleQueryFilterCreator withField(String field) {
        this.field = field;
        return this;
    }

}
 ```

### MongoUpdateBolt
To use the `MongoUpdateBolt`,  you construct an instance of it by specifying Mongo url, collectionName, a `QueryFilterCreator` implementation and a `MongoUpdateMapper` implementation that converts storm tuple to DB document.

 ```java
        MongoUpdateMapper mapper = new SimpleMongoUpdateMapper()
                .withFields("word", "count");

        QueryFilterCreator updateQueryCreator = new SimpleQueryFilterCreator()
                .withField("word");
        
        MongoUpdateBolt updateBolt = new MongoUpdateBolt(url, collectionName, updateQueryCreator, mapper);

        //if a new document should be inserted if there are no matches to the query filter
        //updateBolt.withUpsert(true);

        //whether find all documents according to the query filter
        //updateBolt.withMany(true);
 ```
 
 Or use a anonymous inner class implementation for `QueryFilterCreator`:
 
  ```java
        MongoUpdateMapper mapper = new SimpleMongoUpdateMapper()
                .withFields("word", "count");

        QueryFilterCreator updateQueryCreator = new QueryFilterCreator() {
            @Override
            public Bson createFilter(ITuple tuple) {
                return Filters.gt("count", 3);
            }
        };
        
        MongoUpdateBolt updateBolt = new MongoUpdateBolt(url, collectionName, updateQueryCreator, mapper);

        //if a new document should be inserted if there are no matches to the query filter
        //updateBolt.withUpsert(true);
 ```


## Lookup from Database
The bolt included in this package for selecting data from a database collection.

### MongoLookupMapper
The main API for selecting data in a collection using MongoDB is the `org.apache.storm.mongodb.common.mapper.MongoLookupMapper` interface:

```java
public interface MongoLookupMapper extends Serializable {

    List<Values> toTuple(ITuple input, Document doc);

    void declareOutputFields(OutputFieldsDeclarer declarer);
}
```

### SimpleMongoLookupMapper
`storm-mongodb` includes a general purpose `MongoLookupMapper` implementation called `SimpleMongoLookupMapper` that can converts a Mongo document to a list of storm values.

```java
public class SimpleMongoLookupMapper implements MongoLookupMapper {

    private String[] fields;

    @Override
    public List<Values> toTuple(ITuple input, Document doc) {
        Values values = new Values();

        for(String field : fields) {
            if(input.contains(field)) {
                values.add(input.getValueByField(field));
            } else {
                values.add(doc.get(field));
            }
        }
        List<Values> result = new ArrayList<Values>();
        result.add(values);
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    public SimpleMongoLookupMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }

}
```

### MongoLookupBolt
To use the `MongoLookupBolt`,  you construct an instance of it by specifying Mongo url, collectionName, a `QueryFilterCreator` implementation and a `MongoLookupMapper` implementation that converts a Mongo document to a list of storm values.

 ```java
        MongoLookupMapper mapper = new SimpleMongoLookupMapper()
                .withFields("word", "count");

        QueryFilterCreator filterCreator = new SimpleQueryFilterCreator()
                .withField("word");

        MongoLookupBolt lookupBolt = new MongoLookupBolt(url, collectionName, filterCreator, mapper);
 ```

## Mongo Trident State&MapState
### Trident State
We support trident persistent state that can be used with trident topologies. To create a Mongo persistent trident state you need to initialize it with the url, collectionName, the `MongoMapper` instance. See the example below:

 ```java
        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word", "count");

        MongoState.Options options = new MongoState.Options()
                .withUrl(url)
                .withCollectionName(collectionName)
                .withMapper(mapper);

        StateFactory factory = new MongoStateFactory(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory, fields,
                new MongoStateUpdater(), new Fields());

        TridentState state = topology.newStaticState(factory);
        stream = stream.stateQuery(state, new Fields("word"),
                new MongoStateQuery(), new Fields("columnName", "columnValue"));
        stream.each(new Fields("word", "columnValue"), new PrintFunction(), new Fields());
 ```
 **NOTE**:
 >If there is no unique index provided, trident state inserts in the case of failures may result in duplicate documents.

### Trident MapState
We also support trident `MapState`. To create a Mongo trident `MapState` you need to initialize it with the url, collectionName, the `MongoMapper` and `QueryFilterCreator` instance. See the example below:

 ```java
        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word", "count");

        QueryFilterCreator filterCreator = new SimpleQueryFilterCreator()
                .withField("word");

        MongoMapState.Options options = new MongoMapState.Options();
        options.url = url;
        options.collectionName = collectionName;
        options.mapper = mapper;
        options.queryCreator = filterCreator;

        StateFactory factory = MongoMapState.transactional(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        TridentState state = stream.groupBy(new Fields("word"))
                .persistentAggregate(factory, new Fields("count"), new Sum(), new Fields("sum"));

        stream.stateQuery(state, new Fields("word"), new MapGet(), new Fields("sum"))
                .each(new Fields("word", "sum"), new PrintFunction(), new Fields());
 ```
 

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Committer Sponsors

 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 * Xin Wang ([xinwang@apache.org](mailto:xinwang@apache.org))
 
