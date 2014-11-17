package storm.trident.state;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.utils.Utils;


public class JSONTransactionalSerializer implements Serializer<TransactionalValue> {
    @Override
    public byte[] serialize(TransactionalValue obj) {
        List toSer = new ArrayList(2);
        toSer.add(obj.getTxid());
        toSer.add(obj.getVal());
        try {
            return Utils.to_json(toSer).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TransactionalValue deserialize(byte[] b) {
        try {
            String s = new String(b, "UTF-8");
            List deser = (List) Utils.from_json(s);
            return new TransactionalValue((Long) deser.get(0), deser.get(1));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
