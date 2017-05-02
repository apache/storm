package storm.trident.state;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoNonTransactionalSerializer implements Serializer {

    private final Kryo     _kryo;
    private final Output   _out;
    
    public KryoNonTransactionalSerializer(Kryo kryo, int bufferSize) {
        _kryo = kryo;
        _out = new Output(bufferSize);
    }

    @Override
    public byte[] serialize(Object obj) {
        // Wrap in list to avoid having to specify class of instance being serialized.
        ArrayList li = new ArrayList(1);
        li.add(obj);
        
        _out.clear();
        _kryo.writeObject(_out, li);
        return _out.toBytes();
    }

    @Override
    public Object deserialize(byte[] b) {
        Input in = new Input(b);
        ArrayList o = _kryo.readObject(in, ArrayList.class);
        in.close();
        return o.get(0);
    }

}
