package org.apache.storm.kinesis.spout.test;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.storm.kinesis.spout.RecordToTupleMapper;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

public class TestRecordToTupleMapper implements RecordToTupleMapper, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TestRecordToTupleMapper.class);
    @Override
    public Fields getOutputFields() {
        return new Fields("partitionKey", "sequenceNumber", "data");
    }

    @Override
    public List<Object> getTuple(Record record) {
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        List<Object> tuple = new ArrayList<>();
        tuple.add(record.getPartitionKey());
        tuple.add(record.getSequenceNumber());
        try {
            System.out.println("bytebuffer is " + record.getData());
            String data = decoder.decode(record.getData()).toString();
            System.out.println("data is " + data);
            tuple.add(data);
        } catch (CharacterCodingException e) {
            e.printStackTrace();
            System.out.println("Exception occured. Emitting tuple with empty string data");
            tuple.add("");
        }
        System.out.println("Tuple from record is " + tuple);
        return tuple;
    }
}
