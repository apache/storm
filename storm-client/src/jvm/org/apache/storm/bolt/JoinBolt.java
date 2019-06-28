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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

public class JoinBolt extends BaseWindowedBolt {

    protected final Selector selectorType;
    // Map[StreamName -> JoinInfo]
    protected LinkedHashMap<String, JoinInfo> joinCriteria = new LinkedHashMap<>();
    protected FieldSelector[] outputFields;  // specified via bolt.select() ... used in declaring Output fields
    //    protected String[] dotSeparatedOutputFieldNames; // fieldNames in x.y.z format w/o stream name, used for naming output fields
    protected String outputStreamName;
    // Map[StreamName -> Map[Key -> List<Tuple>]  ]
    HashMap<String, HashMap<Object, ArrayList<Tuple>>> hashedInputs = new HashMap<>(); // holds remaining streams
    private OutputCollector collector;

    /**
     * Calls  JoinBolt(Selector.SOURCE, sourceId, fieldName)
     *
     * @param sourceId  Id of source component (spout/bolt) from which this bolt is receiving data
     * @param fieldName the field to use for joining the stream (x.y.z format)
     */
    public JoinBolt(String sourceId, String fieldName) {
        this(Selector.SOURCE, sourceId, fieldName);
    }


    /**
     * Introduces the first stream to start the join with. Equivalent SQL ... select .... from srcOrStreamId ...
     *
     * @param type          Specifies whether 'srcOrStreamId' refers to stream name/source component
     * @param srcOrStreamId name of stream OR source component
     * @param fieldName     the field to use for joining the stream (x.y.z format)
     */
    public JoinBolt(Selector type, String srcOrStreamId, String fieldName) {
        selectorType = type;

        joinCriteria.put(srcOrStreamId, new JoinInfo(new FieldSelector(srcOrStreamId, fieldName)));
    }

    /**
     * Optional. Allows naming the output stream of this bolt. If not specified, the emits will happen on 'default' stream.
     */
    public JoinBolt withOutputStream(String streamName) {
        this.outputStreamName = streamName;
        return this;
    }

    /**
     * Performs inner Join with the newStream.
     * SQL:
     * <code>from priorStream inner join newStream on newStream.field = priorStream.field1</code>
     * same as:
     * <code>new WindowedQueryBolt(priorStream,field1). join(newStream, field, priorStream);</code>
     * Note: priorStream must be previously joined. Valid ex:
     * <code>new WindowedQueryBolt(s1,k1). join(s2,k2, s1). join(s3,k3, s2);</code>
     * Invalid ex:
     * <code>new WindowedQueryBolt(s1,k1). join(s3,k3, s2). join(s2,k2, s1);</code>
     *
     * @param newStream Either stream name or name of upstream component
     * @param field     the field on which to perform the join
     */
    public JoinBolt join(String newStream, String field, String priorStream) {
        return joinCommon(newStream, field, priorStream, JoinType.INNER);
    }

    /**
     * Performs left Join with the newStream. SQL    :   from stream1  left join stream2  on stream2.field = stream1.field1 same as:   new
     * WindowedQueryBolt(stream1, field1). leftJoin(stream2, field, stream1);
     *
     * <p>Note: priorStream must be previously joined Valid ex:    new WindowedQueryBolt(s1,k1). leftJoin(s2,k2, s1). leftJoin(s3,k3, s2);
     * Invalid ex:  new WindowedQueryBolt(s1,k1). leftJoin(s3,k3, s2). leftJoin(s2,k2, s1);
     *
     * @param newStream Either a name of a stream or an upstream component
     * @param field     the field on which to perform the join
     */
    public JoinBolt leftJoin(String newStream, String field, String priorStream) {
        return joinCommon(newStream, field, priorStream, JoinType.LEFT);
    }

    private JoinBolt joinCommon(String newStream, String fieldDescriptor, String priorStream, JoinType joinType) {
        if (hashedInputs.containsKey(newStream)) {
            throw new IllegalArgumentException("'" + newStream + "' is already part of join. Cannot join with it more than once.");
        }
        hashedInputs.put(newStream, new HashMap<Object, ArrayList<Tuple>>());
        JoinInfo joinInfo = joinCriteria.get(priorStream);
        if (joinInfo == null) {
            throw new IllegalArgumentException("Stream '" + priorStream + "' was not previously declared");
        }

        FieldSelector field = new FieldSelector(newStream, fieldDescriptor);
        joinCriteria.put(newStream, new JoinInfo(field, priorStream, joinInfo, joinType));
        return this;
    }

    /**
     * Specify projection fields. i.e. Specifies the fields to include in the output. e.g: .select("field1, stream2:field2, field3") Nested
     * Key names are supported for nested types: e.g: .select("outerKey1.innerKey1, outerKey1.innerKey2, stream3:outerKey2.innerKey3)" Inner
     * types (non leaf) must be Map<> in order to support nested lookup using this dot notation This selected fields implicitly declare the
     * output fieldNames for the bolt based.
     */
    public JoinBolt select(String commaSeparatedKeys) {
        String[] fieldNames = commaSeparatedKeys.split(",");

        outputFields = new FieldSelector[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            outputFields[i] = new FieldSelector(fieldNames[i]);
        }
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String[] outputFieldNames = new String[outputFields.length];
        for (int i = 0; i < outputFields.length; ++i) {
            outputFieldNames[i] = outputFields[i].getOutputName();
        }
        if (outputStreamName != null) {
            declarer.declareStream(outputStreamName, new Fields(outputFieldNames));
        } else {
            declarer.declare(new Fields(outputFieldNames));
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // initialize the hashedInputs data structure
        int i = 0;
        for (String stream : joinCriteria.keySet()) {
            if (i > 0) {
                hashedInputs.put(stream, new HashMap<Object, ArrayList<Tuple>>());
            }
            ++i;
        }
        if (outputFields == null) {
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
            if (outputStreamName == null) {
                // explicit anchoring emits to corresponding input tuples only, as default window anchoring will anchor them to all
                // tuples in window
                collector.emit(resultRecord.tupleList, outputTuple);
            } else {
                // explicitly anchor emits to corresponding input tuples only, as default window anchoring will anchor them to all tuples
                // in window
                collector.emit(outputStreamName, resultRecord.tupleList, outputTuple);
            }
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
            if (!streamId.equals(firstStream)) {
                Object field = getJoinField(streamId, tuple);
                ArrayList<Tuple> recs = hashedInputs.get(streamId).get(field);
                if (recs == null) {
                    recs = new ArrayList<Tuple>();
                    hashedInputs.get(streamId).put(field, recs);
                }
                recs.add(tuple);

            } else {
                ResultRecord probeRecord = new ResultRecord(tuple, joinCriteria.size() == 1);
                probe.insert(probeRecord);  // first stream's data goes into the probe
            }
        }

        // 2) Join the streams in order of streamJoinOrder
        int i = 0;
        for (String streamName : joinCriteria.keySet()) {
            boolean finalJoin = (i == joinCriteria.size() - 1);
            if (i > 0) {
                probe = doJoin(probe, hashedInputs.get(streamName), joinCriteria.get(streamName), finalJoin);
            }
            ++i;
        }


        return probe;
    }

    // Dispatches to the right join method (inner/left/right/outer) based on the joinInfo.joinType
    protected JoinAccumulator doJoin(JoinAccumulator probe, HashMap<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo,
                                     boolean finalJoin) {
        final JoinType joinType = joinInfo.getJoinType();
        switch (joinType) {
            case INNER:
                return doInnerJoin(probe, buildInput, joinInfo, finalJoin);
            case LEFT:
                return doLeftJoin(probe, buildInput, joinInfo, finalJoin);
            case RIGHT:
            case OUTER:
            default:
                throw new RuntimeException("Unsupported join type : " + joinType.name());
        }
    }

    // inner join - core implementation
    protected JoinAccumulator doInnerJoin(JoinAccumulator probe, Map<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo,
                                          boolean finalJoin) {
        String[] probeKeyName = joinInfo.getOtherField();
        JoinAccumulator result = new JoinAccumulator();
        FieldSelector fieldSelector = new FieldSelector(joinInfo.other.getStreamName(), probeKeyName);
        for (ResultRecord rec : probe.getRecords()) {
            Object probeKey = rec.getField(fieldSelector);
            if (probeKey != null) {
                ArrayList<Tuple> matchingBuildRecs = buildInput.get(probeKey);
                if (matchingBuildRecs != null) {
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
    protected JoinAccumulator doLeftJoin(JoinAccumulator probe, Map<Object, ArrayList<Tuple>> buildInput, JoinInfo joinInfo,
                                         boolean finalJoin) {
        String[] probeKeyName = joinInfo.getOtherField();
        JoinAccumulator result = new JoinAccumulator();
        FieldSelector fieldSelector = new FieldSelector(joinInfo.other.getStreamName(), probeKeyName);
        for (ResultRecord rec : probe.getRecords()) {
            Object probeKey = rec.getField(fieldSelector);
            if (probeKey != null) {
                ArrayList<Tuple> matchingBuildRecs = buildInput.get(probeKey); // ok if its return null
                if (matchingBuildRecs != null && !matchingBuildRecs.isEmpty()) {
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

    // Identify the join field for the stream, and look it up in 'tuple'. field can be nested field:  outerKey.innerKey
    private Object getJoinField(String streamId, Tuple tuple) {
        JoinInfo ji = joinCriteria.get(streamId);
        if (ji == null) {
            throw new RuntimeException("Join information for '" + streamId + "' not found. Check the join clauses.");
        }
        return lookupField(ji.getJoinField(), tuple);
    }

    // Returns either the source component name or the stream name for the tuple
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

    // Performs projection on the tuples based on 'projectionFields'
    protected ArrayList<Object> doProjection(ArrayList<Tuple> tuples, FieldSelector[] projectionFields) {
        ArrayList<Object> result = new ArrayList<>(projectionFields.length);
        // Todo: optimize this computation... perhaps inner loop can be outside to avoid rescanning tuples
        for (int i = 0; i < projectionFields.length; i++) {
            boolean missingField = true;
            for (Tuple tuple : tuples) {
                Object field = lookupField(projectionFields[i], tuple);
                if (field != null) {
                    result.add(field);
                    missingField = false;
                    break;
                }
            }
            if (missingField) { // add a null for missing fields (usually in case of outer joins)
                result.add(null);
            }
        }
        return result;
    }

    // Extract the field from tuple. Field may be nested field (x.y.z)
    protected Object lookupField(FieldSelector fieldSelector, Tuple tuple) {

        // very stream name matches, it stream name was specified
        if (fieldSelector.streamName != null
                && !fieldSelector.streamName.equalsIgnoreCase(getStreamSelector(tuple))) {
            return null;
        }

        Object curr = null;
        for (int i = 0; i < fieldSelector.field.length; i++) {
            if (i == 0) {
                if (tuple.contains(fieldSelector.field[i])) {
                    curr = tuple.getValueByField(fieldSelector.field[i]);
                } else {
                    return null;
                }
            } else {
                curr = ((Map) curr).get(fieldSelector.field[i]);
                if (curr == null) {
                    return null;
                }
            }
        }
        return curr;
    }

    @Override
    public JoinBolt withWindow(Count windowLength, Count slidingInterval) {
        return (JoinBolt) super.withWindow(windowLength, slidingInterval);
    }

    @Override
    public JoinBolt withWindow(Count windowLength, Duration slidingInterval) {
        return (JoinBolt) super.withWindow(windowLength, slidingInterval);
    }

    @Override
    public JoinBolt withWindow(Duration windowLength, Count slidingInterval) {
        return (JoinBolt) super.withWindow(windowLength, slidingInterval);
    }

    @Override
    public JoinBolt withWindow(Duration windowLength, Duration slidingInterval) {
        return (JoinBolt) super.withWindow(windowLength, slidingInterval);
    }

    @Override
    public JoinBolt withWindow(Count windowLength) {
        return (JoinBolt) super.withWindow(windowLength);
    }

    @Override
    public JoinBolt withWindow(Duration windowLength) {
        return (JoinBolt) super.withWindow(windowLength);
    }

    // Boilerplate overrides to cast result from base type to JoinBolt, so user doesn't have to
    // down cast when invoking these methods

    @Override
    public JoinBolt withTumblingWindow(Count count) {
        return (JoinBolt) super.withTumblingWindow(count);
    }

    @Override
    public JoinBolt withTumblingWindow(Duration duration) {
        return (JoinBolt) super.withTumblingWindow(duration);
    }

    @Override
    public JoinBolt withTimestampField(String fieldName) {
        return (JoinBolt) super.withTimestampField(fieldName);
    }

    @Override
    public JoinBolt withTimestampExtractor(TimestampExtractor timestampExtractor) {
        return (JoinBolt) super.withTimestampExtractor(timestampExtractor);
    }

    @Override
    public JoinBolt withLateTupleStream(String streamId) {
        return (JoinBolt) super.withLateTupleStream(streamId);
    }

    @Override
    public BaseWindowedBolt withLag(Duration duration) {
        return (JoinBolt) super.withLag(duration);
    }

    @Override
    public BaseWindowedBolt withWatermarkInterval(Duration interval) {
        return (JoinBolt) super.withWatermarkInterval(interval);
    }

    // Use streamId, source component name OR field in tuple to distinguish incoming tuple streams
    public enum Selector {
        STREAM, SOURCE
    }

    protected enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        OUTER
    }

    /**
     * Describes how to join the other stream with the current stream.
     */
    protected static class JoinInfo implements Serializable {
        static final long serialVersionUID = 1L;

        private JoinType joinType;        // nature of join
        private FieldSelector field;           // field for the current stream
        private FieldSelector other;      // field for the other (2nd) stream


        public JoinInfo(FieldSelector field) {
            this.joinType = null;
            this.field = field;
            this.other = null;
        }

        public JoinInfo(FieldSelector field, String otherStream, JoinInfo otherStreamJoinInfo, JoinType joinType) {
            this.joinType = joinType;
            this.field = field;
            this.other = new FieldSelector(otherStream, otherStreamJoinInfo.field.getOutputName());
        }

        public FieldSelector getJoinField() {
            return field;
        }

        public String getOtherStream() {
            return other.getStreamName();
        }

        public String[] getOtherField() {
            return other.getField();
        }

        public JoinType getJoinType() {
            return joinType;
        }

    } // class JoinInfo

    protected static class FieldSelector implements Serializable {
        String streamName;    // can be null;
        String[] field;       // nested field "x.y.z"  becomes => String["x","y","z"]
        String outputName;    // either "stream1:x.y.z" or "x.y.z" depending on whether stream name is present.

        public FieldSelector(String fieldDescriptor) {  // sample fieldDescriptor = "stream1:x.y.z"
            int pos = fieldDescriptor.indexOf(':');

            if (pos > 0) {  // stream name is specified
                streamName = fieldDescriptor.substring(0, pos).trim();
                outputName = fieldDescriptor.trim();
                field = fieldDescriptor.substring(pos + 1, fieldDescriptor.length()).split("\\.");
                return;
            }

            // stream name unspecified
            streamName = null;
            if (pos == 0) {
                outputName = fieldDescriptor.substring(1, fieldDescriptor.length()).trim();

            } else if (pos < 0) {
                outputName = fieldDescriptor.trim();
            }
            field = outputName.split("\\.");
        }

        /**
         * Constructor.
         *
         * @param stream          name of stream
         * @param fieldDescriptor Simple fieldDescriptor like "x.y.z" and w/o a 'stream1:' stream qualifier.
         */
        public FieldSelector(String stream, String fieldDescriptor) {
            this(fieldDescriptor);
            if (fieldDescriptor.indexOf(":") >= 0) {
                throw new IllegalArgumentException("Not expecting stream qualifier ':' in '" + fieldDescriptor
                                                   + "'. Stream name '" + stream + "' is implicit in this context");
            }
            this.streamName = stream;
        }

        public FieldSelector(String stream, String[] field) {
            this(stream, String.join(".", field));
        }


        public String getStreamName() {
            return streamName;
        }

        public String[] getField() {
            return field;
        }

        public String getOutputName() {
            return toString();
        }

        @Override
        public String toString() {
            return outputName;
        }
    }

    // Join helper to concat fields to the record
    protected class ResultRecord {

        ArrayList<Tuple> tupleList = new ArrayList<>(); // contains one Tuple per Stream being joined
        ArrayList<Object> outFields = null; // refs to fields that will be part of output fields

        // 'generateOutputFields' enables us to avoid projection unless it is the final stream being joined
        public ResultRecord(Tuple tuple, boolean generateOutputFields) {
            tupleList.add(tuple);
            if (generateOutputFields) {
                outFields = doProjection(tupleList, outputFields);
            }
        }

        public ResultRecord(ResultRecord lhs, Tuple rhs, boolean generateOutputFields) {
            if (lhs != null) {
                tupleList.addAll(lhs.tupleList);
            }
            if (rhs != null) {
                tupleList.add(rhs);
            }
            if (generateOutputFields) {
                outFields = doProjection(tupleList, outputFields);
            }
        }

        public ArrayList<Object> getOutputFields() {
            return outFields;
        }


        // 'stream' cannot be null,
        public Object getField(FieldSelector fieldSelector) {
            for (Tuple tuple : tupleList) {
                Object result = lookupField(fieldSelector, tuple);
                if (result != null) {
                    return result;
                }
            }
            return null;
        }
    }

    protected class JoinAccumulator {
        ArrayList<ResultRecord> records = new ArrayList<>();

        public void insert(ResultRecord tuple) {
            records.add(tuple);
        }

        public Collection<ResultRecord> getRecords() {
            return records;
        }
    }
}
