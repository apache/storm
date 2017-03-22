package org.apache.storm.eventhubs.spout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by rabaner on 3/22/2017.
 */
public class Serializedeserializeutil {

    public static byte[] serialize(Object obj) throws IOException {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            try(ObjectOutputStream o = new ObjectOutputStream(b)){
                o.writeObject(obj);
                o.close();
            }
            return b.toByteArray();
        }
    }
}
