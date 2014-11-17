package storm.trident.state;

import java.io.UnsupportedEncodingException;

import backtype.storm.utils.Utils;


public class JSONNonTransactionalSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        try {
            return Utils.to_json(obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] b) {
        try {
            return Utils.from_json(new String(b, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
