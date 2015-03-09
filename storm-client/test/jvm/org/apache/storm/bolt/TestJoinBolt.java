/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.Assert;
import org.junit.Test;

public class TestJoinBolt {
    String[] userFields = { "userId", "name", "city" };
    Object[][] users = {
        { 1, "roshan", "san jose" },
        { 2, "harsha", "santa clara" },
        { 3, "siva", "dublin" },
        { 4, "hugo", "san mateo" },
        { 5, "suresh", "sunnyvale" },
        { 6, "guru", "palo alto" },
        { 7, "arun", "bengaluru" },
        { 8, "satish", "mumbai" },
        { 9, "mani", "bengaluru" },
        { 10, "priyank", "seattle" }
    };

    String[] orderFields = { "orderId", "userId", "itemId", "price" };

    Object[][] orders = {
        { 11, 2, 21, 7 },
        { 12, 2, 22, 3 },
        { 13, 3, 23, 4 },
        { 14, 4, 24, 5 },
        { 15, 5, 25, 2 },
        { 16, 6, 26, 7 },
        { 17, 6, 27, 4 },
        { 18, 7, 28, 2 },
        { 19, 8, 29, 9 }
    };

    String[] storeFields = { "storeId", "storeName", "city" };
    Object[][] stores = {
        { 1, "store1", "san jose" },
        { 2, "store2", "santa clara" },
        { 3, "store3", "dublin" },
        { 4, "store4", "san mateo" },
        { 5, "store5", "bengaluru" },
    };

    String[] cityFields = { "cityId", "cityName", "country" };
    Object[][] cities = {
        { 1, "san jose", "US" },
        { 2, "santa clara", "US" },
        { 3, "dublin", "US" },
        { 4, "san mateo", "US" },
        { 5, "sunnyvale", "US" },
        { 6, "palo alto", "US" },
        { 7, "bengaluru", "India" },
        { 8, "mumbai", "India" },
        { 9, "chennai", "India" }
    };

    private static void printResults(MockCollector collector) {
        int counter = 0;
        for (List<Object> rec : collector.actualResults) {
            System.out.print(++counter + ") ");
            for (Object field : rec) {
                System.out.print(field + ", ");
            }
            System.out.println("");
        }
    }

    private static TupleWindow makeTupleWindow(ArrayList<Tuple> stream) {
        return new TupleWindowImpl(stream, null, null);
    }

    private static TupleWindow makeTupleWindow(ArrayList<Tuple>... streams) {
        ArrayList<Tuple> combined = null;
        for (int i = 0; i < streams.length; i++) {
            if (i == 0) {
                combined = new ArrayList<>(streams[0]);
            } else {
                combined.addAll(streams[i]);
            }
        }
        return new TupleWindowImpl(combined, null, null);
    }

    private static ArrayList<Tuple> makeStream(String streamName, String[] fieldNames, Object[][] data, String srcComponentName) {
        ArrayList<Tuple> result = new ArrayList<>();
        MockContext mockContext = new MockContext(fieldNames);

        for (Object[] record : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(record), srcComponentName, 0, streamName);
            result.add(rec);
        }

        return result;
    }

    private static ArrayList<Tuple> makeNestedEventsStream(String streamName, String[] fieldNames, Object[][] records
        , String srcComponentName) {

        MockContext mockContext = new MockContext(new String[]{ "outer" });
        ArrayList<Tuple> result = new ArrayList<>(records.length);

        // convert each record into a HashMap using fieldNames as keys
        for (Object[] record : records) {
            HashMap<String, Object> recordMap = new HashMap<>(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                recordMap.put(fieldNames[i], record[i]);
            }

            ArrayList<Object> tupleValues = new ArrayList<>(1);
            tupleValues.add(recordMap);
            TupleImpl tuple = new TupleImpl(mockContext, tupleValues, srcComponentName, 0, streamName);
            result.add(tuple);
        }

        return result;
    }

    @Test
    public void testTrivial() throws Exception {
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders, "ordersSpout");
        TupleWindow window = makeTupleWindow(orderStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "orders", orderFields[0])
            .select("orderId,userId,itemId,price");
        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(orderStream.size(), collector.actualResults.size());
    }

    @Test
    public void testNestedKeys() throws Exception {
        ArrayList<Tuple> userStream = makeNestedEventsStream("users", userFields, users, "usersSpout");
        TupleWindow window = makeTupleWindow(userStream);
        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", "outer.userId")
            .select("outer.name, outer.city");
        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(userStream.size(), collector.actualResults.size());
    }

    @Test
    public void testProjection_FieldsWithStreamName() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> storeStream = makeStream("stores", storeFields, stores, "storesSpout");

        TupleWindow window = makeTupleWindow(storeStream, userStream);

        // join users and stores on city name
        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[2])
            .join("stores", "city", "users")
            .select("userId,name,storeName,users:city,stores:city");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(storeStream.size() + 1, collector.actualResults.size());
        // ensure 5 fields per tuple and no null fields
        for (List<Object> tuple : collector.actualResults) {
            Assert.assertEquals(5, tuple.size());
            for (Object o : tuple) {
                Assert.assertNotNull(o);
            }
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders, "ordersSpout");
        TupleWindow window = makeTupleWindow(orderStream, userStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[0])
            .join("orders", "userId", "users")
            .select("userId,name,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(orders.length, collector.actualResults.size());
    }

    @Test
    public void testLeftJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> orderStream = makeStream("orders", orderFields, orders, "ordersSpout");
        TupleWindow window = makeTupleWindow(orderStream, userStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[0])
            .leftJoin("orders", "userId", "users")
            .select("userId,name,price");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(12, collector.actualResults.size());
    }

    @Test
    public void testThreeStreamInnerJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> storesStream = makeStream("stores", storeFields, stores, "storesSpout");
        ArrayList<Tuple> cityStream = makeStream("cities", cityFields, cities, "citiesSpout");

        TupleWindow window = makeTupleWindow(userStream, storesStream, cityStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[2])
            .join("stores", "city", "users")
            .join("cities", "cityName", "stores")
            .select("name,storeName,city,country");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(6, collector.actualResults.size());

    }

    @Test
    public void testThreeStreamLeftJoin_1() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> storesStream = makeStream("stores", storeFields, stores, "storesSpout");
        ArrayList<Tuple> cityStream = makeStream("cities", cityFields, cities, "citiesSpout");

        TupleWindow window = makeTupleWindow(userStream, cityStream, storesStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[2])
            .leftJoin("stores", "city", "users")
            .leftJoin("cities", "cityName", "users")
            .select("name,storeName,city,country");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(users.length, collector.actualResults.size());
    }

    @Test
    public void testThreeStreamLeftJoin_2() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> storesStream = makeStream("stores", storeFields, stores, "storesSpout");
        ArrayList<Tuple> cityStream = makeStream("cities", cityFields, cities, "citiesSpout");

        TupleWindow window = makeTupleWindow(userStream, cityStream, storesStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", "city")
            .leftJoin("stores", "city", "users")
            .leftJoin("cities", "cityName", "stores")  // join against diff stream compared to testThreeStreamLeftJoin_1
            .select("name,storeName,city,country");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(stores.length + 1, collector.actualResults.size()); // stores.length+1 as 2 users in Bengaluru
    }

    @Test
    public void testThreeStreamMixedJoin() throws Exception {
        ArrayList<Tuple> userStream = makeStream("users", userFields, users, "usersSpout");
        ArrayList<Tuple> storesStream = makeStream("stores", storeFields, stores, "storesSpout");
        ArrayList<Tuple> cityStream = makeStream("cities", cityFields, cities, "citiesSpout");

        TupleWindow window = makeTupleWindow(userStream, cityStream, storesStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "users", userFields[2])
            .join("stores", "city", "users")
            .leftJoin("cities", "cityName", "users")
            .select("name,storeName,city,country");

        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        printResults(collector);
        Assert.assertEquals(stores.length + 1, collector.actualResults.size()); // stores.length+1 as 2 users in Bengaluru
    }

    static class MockCollector extends OutputCollector {
        public ArrayList<List<Object>> actualResults = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
            actualResults.add(tuple);
            return null;
        }

    } // class MockCollector

    static class MockContext extends GeneralTopologyContext {

        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, new HashMap<>(), null, null, null, null);
            this.fields = new Fields(fieldNames);
        }

        @Override
        public String getComponentId(int taskId) {
            return "component";
        }

        @Override
        public Fields getComponentOutputFields(String componentId, String streamId) {
            return fields;
        }

    }
}
