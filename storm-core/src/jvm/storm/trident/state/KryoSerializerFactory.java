package storm.trident.state;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;

import backtype.storm.serialization.SerializationFactory;

public class KryoSerializerFactory implements SerializerFactory {

    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final int        _bufferSize;
    
    public KryoSerializerFactory() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public KryoSerializerFactory(int bufferSize) {
        _bufferSize = bufferSize;
    }

    @Override
    public Serializer makeSerializer(Map conf, StateType stateType) {
        Kryo kryo = SerializationFactory.getKryo(conf);
        
        if (stateType == StateType.NON_TRANSACTIONAL) {
            return new KryoNonTransactionalSerializer(kryo, _bufferSize);
        }

        if (stateType == StateType.TRANSACTIONAL) {
            return new KryoTransactionalSerializer(kryo, _bufferSize);
        }

        return new KryoOpaqueSerializer(kryo, _bufferSize);
    }

}
