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
package org.apache.storm.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class JoinBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    // Map[StreamName -> Map[Key -> List<Tuple>]  ]
    HashMap<String, HashMap<Object, ArrayList<Tuple> >> hashedInputs = new HashMap<>(); // holds remaining streams

    // Map[StreamName -> JoinInfo]
    protected LinkedHashMap<String, JoinInfo> joinCriteria = new LinkedHashMap<>();
    protected String[][] outputKeys;  // specified via bolt.select() ... used in declaring Output fields
    protected String[] dotSeparatedOutputKeyNames; // flattened (de nested) keyNames, used for naming output fields
    protected String outputStreamName;

    // Use streamId, source component name OR field in tuple to distinguish incoming tuple streams
    public enum Selector { STREAM, SOURCE }
    protected final Selector selectorType;


    /**
     * StreamId to start the join with. Equivalent SQL ...
     *       select .... from streamId ...
     * @param type Specifies whether 'streamId' refers to stream name/source component
     * @param streamId name of stream/source component
     * @param key the fieldName to use as key for the stream (used for performing joins)
     */
    public JoinBolt(Selector type, String streamId, String key) {
        selectorType = type;
        joinCriteria.put(streamId, new JoinInfo(key) );
    }

    /**
     * Defines the name of the output stream
     */
    public JoinBolt withOutputStream(String streamName) {
        this.outputStreamName = streamName;
        return this;
    }

    /**
     * Performs inner Join.
     *  SQL    :   from priorStream inner join newStream on newStream.key = priorStream.key1
     *  same as:   new WindowedQueryBolt(priorStream,key1). join(newStream, key, priorStream);
     *
     *  Note: priorStream must be previously joined.
     *    Valid ex:    new WindowedQueryBolt(s1,k1). join(s2,k2, s1). join(s3,k3, s2);
     *    Invalid ex:  new WindowedQueryBolt(s1,k1). join(s3,k3, s2). join(s2,k2, s1);
     */
    public JoinBolt join(String newStream, String key, String priorStream) {
        return joinCommon(newStream, key, priorStream, JoinType.INNER);
    }

    /**
     * Performs left Join.
     *  SQL    :   from stream1  left join stream2  on stream2.key = stream1.key1
     *  same as:   new  WindowedQueryBolt(stream1, key1). leftJoin(stream2, key, stream1);
     *
     *  Note: priorStream must be previously joined
     *    Valid ex:    new WindowedQueryBolt(s1,k1). leftJoin(s2,k2, s1). leftJoin(s3,k3, s2);
     *    Invalid ex:  new WindowedQueryBolt(s1,k1). leftJoin(s3,k3, s2). leftJoin(s2,k2, s1);
     */
    public JoinBolt leftJoin(String newStream, String key, String priorStream) {
        return joinCommon(newStream, key, priorStream, JoinType.LEFT);
    }

    private JoinBolt joinCommon(String newStream, String key, String priorStream, JoinType joinType) {
        hashedInputs.put(newStream, new HashMap<Object, ArrayList<Tuple>>());
        JoinInfo joinInfo = joinCriteria.get(priorStream);
        if( joinInfo==null )
            throw new IllegalArgumentException("Stream '" + priorStream + "' was not previously declared");
        joinCriteria.put(newStream, new JoinInfo(key, priorStream, joinInfo, joinType) );
        return this;
    }

    /**
     * Specify projection keys. i.e. Specifies the keys to include in the output.
     *      e.g: .select("key1, key2, key3")
     * Nested Key names are supported for nested types:
     *      e.g: .select("outerKey1.innerKey1, outerKey1.innerKey2, outerKey2.innerKey3)"
     * Inner types (non leaf) must be Map<> in order to support lookup by key name
     * This selected keys implicitly declare the output fieldNames for the bolt based.
     * @param commaSeparatedKeys
     * @return
     */
    public JoinBolt select(String commaSeparatedKeys) {
        String[] keyNames = commaSeparatedKeys.split(",");
        dotSeparatedOutputKeyNames = new String[keyNames.length];
        outputKeys = new String[keyNames.length][];
        for (int i = 0; i < keyNames.length; i++) {
            dotSeparatedOutputKeyNames[i] = keyNames[i].trim();
            outputKeys[i] = dotSeparatedOutputKeyNames[i].split("\\.");
        }
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (outputStreamName!=null) {
            declarer.declareStream(outputStreamName, new Fields(dotSeparatedOutputKeyNames));
        } else {
            declarer.declare(new Fields(dotSeparatedOutputKeyNames));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // initialize the hashedInputs data structure
        int i=0;
        for ( String stream : joinCriteria.keySet() ) {
            if(i>0) {
                hashedInputs.put(stream, new HashMap<Object, ArrayList<Tuple>>());
            }
            ++i;
        }
        if(outputKeys==null) {
            throw new IllegalArgumentException("Must specify output fields via .select() method.");
        }
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        // 1) Perform Join
        List<Tuple> currentWindow = inputWindow.get();
        JoinAccumulator joinResult = hashJoin(currentWindow);

        // 2) Emit results
        for (ResultRecord resultRecord : joinResult.getRecords()) {
            ArrayList<Object> outputTuple = resultRecord.getOutputFields();
            if ( outputStreamName==null )
                collector.emit( outputTuple );
            else
                collector.emit( outputStreamName, outputTuple );
        }
    }

    private void clearHashedInputs() {
        for (HashMap<Object, ArrayList<Tuple>> mappings : hashedInputs.values()) {
            mappings.clear();
        }
    }

    protected JoinAccumulator hashJoin(List<Tuple> tuples) {
        clearHashedInputs();

        JoinAccumulator probe = new JoinAccumulator();

        // 1) Build phase - Segregate tuples in the Window into streams.
        //    First stream's tuples go into probe, rest into HashMaps in hashedInputs
        String firstStream = joinCriteria.keySet().iterator().next();
        for (Tuple tuple : tuples) {
            String streamId = getStreamSelector(tuple);
            if ( ! streamId.equals(firstStream) ) {
                Object key = getKeyField(streamId, tuple);
                ArrayList<Tuple> recs = hashedInputs.get(streamId).get(key);
                if(recs == null) {
                    recs = new ArrayList<Tuple>();
                    hashedInputs.get(streamId).put(key, recs);
                }
                recs.add(tuple);

            }  else {
                ResultRecord probeRecord = new ResultRecord(tuple, joinCriteria.size() == 1);
                probe.insert( probeRecord );  // first stream's data goes into the probe
            }
        }

        // 2) Join the streams in order of streamJoinOrder
        int i=0;
        for (String streamName : joinCriteria.keySet() ) {
            boolean finalJoin = (i==joinCriteria.size()-1);
            if(i>0) {
                probe = doJoin(probe, hashedInputs.get(streamName), joinCriteria.get(streamName), finalJoin);
            }
            ++i;
        }


        return probe;
    }

    // Dispatches to the right join method (inner/left/right/outer) based on the joinInfo.joinType
    protected JoinAccumulator doJoin(JoinAccumulator probe, HashMap<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo, boolean finalJoin) {
        final JoinType joinType = joinInfo.getJoinType();
        switch ( joinType ) {
            case INNER:
                return doInnerJoin(probe, buildInput, joinInfo, finalJoin);
            case LEFT:
                return doLeftJoin(probe, buildInput, joinInfo, finalJoin);
            case RIGHT:
            case OUTER:
            default:
                throw new RuntimeException("Unsupported join type : " + joinType.name() );
        }
    }

    // inner join - core implementation
    protected JoinAccumulator doInnerJoin(JoinAccumulator probe, Map<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo, boolean finalJoin) {
        String[] probeKeyName = joinInfo.getOtherKey();
        JoinAccumulator result = new JoinAccumulator();
        for (ResultRecord rec : probe.getRecords()) {
            Object probeKey = rec.getField(joinInfo.otherStream, probeKeyName);
            if (probeKey!=null) {
                ArrayList<Tuple> matchingBuildRecs = buildInput.get(probeKey);
                if(matchingBuildRecs!=null) {
                    for (Tuple matchingRec : matchingBuildRecs) {
                        ResultRecord mergedRecord = new ResultRecord(rec, matchingRec, finalJoin);
                        result.insert(mergedRecord);
                    }
                }
            }
        }
        return result;
    }

    // left join - core implementation
    protected JoinAccumulator doLeftJoin(JoinAccumulator probe, Map<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo, boolean finalJoin) {
        String[] probeKeyName = joinInfo.getOtherKey();
        JoinAccumulator result = new JoinAccumulator();
        for (ResultRecord rec : probe.getRecords()) {
            Object probeKey = rec.getField(joinInfo.otherStream, probeKeyName);
            if (probeKey!=null) {
                ArrayList<Tuple> matchingBuildRecs = buildInput.get(probeKey); // ok if its return null
                if (matchingBuildRecs!=null && !matchingBuildRecs.isEmpty() ) {
                    for (Tuple matchingRec : matchingBuildRecs) {
                        ResultRecord mergedRecord = new ResultRecord(rec, matchingRec, finalJoin);
                        result.insert(mergedRecord);
                    }
                } else {
                    ResultRecord mergedRecord = new ResultRecord(rec, null, finalJoin);
                    result.insert(mergedRecord);
                }

            }
        }
        return result;
    }


    // Identify the key for the stream, and look it up in 'tuple'. key can be nested key:  outerKey.innerKey
    private Object getKeyField(String streamId, Tuple tuple) {
        JoinInfo ji = joinCriteria.get(streamId);
        if(ji==null) {
            throw new RuntimeException("Join information for '" + streamId + "' not found. Check the join clauses.");
        }
        return getNestedField(ji.getNestedKeyName(), tuple);
    }

    // Steps down into a nested tuple based on the nestedKeyName
    protected static Object getNestedField(String[] nestedKeyName, Tuple tuple) {
        Object curr = null;
        for (int i=0; i < nestedKeyName.length; i++) {
            if (i==0) {
                if (tuple.contains(nestedKeyName[i]) )
                    curr = tuple.getValueByField(nestedKeyName[i]);
                else
                    return null;
            }  else  {
                curr = ((Map) curr).get(nestedKeyName[i]);
                if (curr==null)
                    return null;
            }
        }
        return curr;
    }


    private String getStreamSelector(Tuple ti) {
        switch (selectorType) {
            case STREAM:
                return ti.getSourceStreamId();
            case SOURCE:
                return ti.getSourceComponent();
            default:
                throw new RuntimeException(selectorType + " stream selector type not yet supported");
        }
    }


    protected enum JoinType {INNER, LEFT, RIGHT, OUTER}

    /** Describes how to join the other stream with the current stream */
    protected static class JoinInfo implements Serializable {
        final static long serialVersionUID = 1L;

        public String[] nestedKeyName;    // nested  key name for the current stream:  outer.inner -> { "outer", "inner }
        public String   otherStream;      // name of the other stream to join with
        public String[] otherKey;         // key name of the other stream
        public JoinType joinType;         // nature of join

        // nestedKeys uses  dot separated key names...  outer.inner.innermostKey
        public JoinInfo(String nestedKey) {
            this.nestedKeyName = nestedKey.split("\\.");
            this.otherStream = null;
            this.otherKey = null;
            this.joinType = null;
        }
        public JoinInfo(String nestedKey, String otherStream, JoinInfo otherStreamJoinInfo,  JoinType joinType) {
            this.nestedKeyName = nestedKey.split("\\.");
            this.otherStream = otherStream;
            this.otherKey = otherStreamJoinInfo.nestedKeyName;
            this.joinType = joinType;
        }

        public String[] getNestedKeyName() {
            return nestedKeyName;
        }

        public String getOtherStream() {
            return otherStream;
        }

        public String[] getOtherKey() {
            return otherKey;
        }

        public JoinType getJoinType() {
            return joinType;
        }

    } // class JoinInfo

    // Join helper to concat fields to the record
    protected class ResultRecord {

        ArrayList<Tuple> tupleList = new ArrayList<>(); // contains one Tuple per Stream being joined
        ArrayList<Object> outputFields = null; // refs to fields that will be part of output fields

        // 'generateOutputFields' enables us to avoid projection unless it is the final stream being joined
        public ResultRecord(Tuple tuple, boolean generateOutputFields) {
            tupleList.add(tuple);
            if(generateOutputFields) {
                outputFields = doProjection(tupleList, outputKeys);
            }
        }

        public ResultRecord(ResultRecord lhs, Tuple rhs, boolean generateOutputFields) {
            if(lhs!=null)
                tupleList.addAll(lhs.tupleList);
            if(rhs!=null)
                tupleList.add(rhs);
            if(generateOutputFields) {
                outputFields = doProjection(tupleList, outputKeys);
            }
        }

        public ArrayList<Object> getOutputFields() {
            return outputFields;
        }

        public Object getField(String stream, String[] nestedFieldName) {
            for (Tuple tuple : tupleList) {
                if(getStreamSelector(tuple).equals(stream))
                    return getNestedField(nestedFieldName, tuple);
            }
            return null;
        }
    }

    protected class JoinAccumulator {
        ArrayList<ResultRecord> records = new ArrayList<>();

        public void insert(ResultRecord tuple) {
            records.add( tuple );
        }

        public Collection<ResultRecord> getRecords() {
            return records;
        }
    }

    // Performs projection on the tuples based on the 'projectionKeys'
    protected ArrayList<Object> doProjection(ArrayList<Tuple> tuples, String[][] projectionKeys) {
        ArrayList<Object> result = new ArrayList<>(projectionKeys.length);
        // Todo: optimize this computation... perhaps inner loop can be outside to avoid rescanning tuples
        for ( int i = 0; i < projectionKeys.length; i++ ) {
            boolean missingField = true;
            for ( Tuple tuple : tuples ) {
                Object field = getNestedField(projectionKeys[i], tuple ) ;
                if (field != null) {
                    result.add(field);
                    missingField=false;
                    break;
                }
            }
            if(missingField) { // add a null for missing fields (usually in case of outer joins)
                result.add(null);
            }
        }
        return result;
    }
}