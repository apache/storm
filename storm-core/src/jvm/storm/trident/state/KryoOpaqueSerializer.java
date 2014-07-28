package storm.trident.state;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoOpaqueSerializer implements Serializer<OpaqueValue> {

    private final Kryo   _kryo;
    private final Output _out;

    public KryoOpaqueSerializer(Kryo kryo, int bufferSize) {
        _kryo = kryo;
        _out = new Output(bufferSize);
    }

    @Override
    public byte[] serialize(OpaqueValue obj) {
        ArrayList li = new ArrayList(3);
        li.add(obj.currTxid);
        li.add(obj.curr);
        li.add(obj.prev);

        _out.clear();
        _kryo.writeObject(_out, li);
        return _out.toBytes();
    }

    @Override
    public OpaqueValue deserialize(byte[] b) {
        Input in = new Input(b);
        ArrayList li = _kryo.readObject(in, ArrayList.class);
        in.close();
        return new OpaqueValue((Long) li.get(0), li.get(1), li.get(2));
    }

}
