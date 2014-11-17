package storm.trident.state;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.utils.Utils;


public class JSONOpaqueSerializer implements Serializer<OpaqueValue> {

    @Override
    public byte[] serialize(OpaqueValue obj) {
        List toSer = new ArrayList(3);
        toSer.add(obj.currTxid);
        toSer.add(obj.curr);
        toSer.add(obj.prev);
        try {
            return Utils.to_json(toSer).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OpaqueValue deserialize(byte[] b) {
        try {
            String s = new String(b, "UTF-8");
            List deser = (List) Utils.from_json(s);
            return new OpaqueValue((Long) deser.get(0), deser.get(1), deser.get(2));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
