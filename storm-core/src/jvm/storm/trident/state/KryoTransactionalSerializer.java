package storm.trident.state;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoTransactionalSerializer implements Serializer<TransactionalValue> {

    private final Kryo   _kryo;
    private final Output _out;

    public KryoTransactionalSerializer(Kryo kryo, int bufferSize) {
        _kryo = kryo;
        _out = new Output(bufferSize);
    }

    @Override
    public byte[] serialize(TransactionalValue obj) {
        ArrayList li = new ArrayList(2);
        li.add(obj.getTxid());
        li.add(obj.getVal());

        _out.clear();
        _kryo.writeObject(_out, li);
        return _out.toBytes();
    }

    @Override
    public TransactionalValue deserialize(byte[] b) {
        Input in = new Input(b);
        ArrayList li = _kryo.readObject(in, ArrayList.class);
        in.close();
        return new TransactionalValue((Long) li.get(0), li.get(1));
    }

}
