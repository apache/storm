package org.apache.storm.kafka;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.tuple.Fields;

public class ByteKeyValueScheme implements KeyValueScheme {

    private final RawScheme rawScheme = new RawScheme();

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        final ArrayList<Object> result = new ArrayList<>(2);
        result.addAll(rawScheme.deserialize(key));
        result.addAll(rawScheme.deserialize(value));
        return result;
    }

    @Override
    public List<Object> deserialize(ByteBuffer value) {
        return rawScheme.deserialize(value);
    }


    @Override
    public Fields getOutputFields() {
        return new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE);
    }
}
