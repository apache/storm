package storm.kafka;

import backtype.storm.Config;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
* Created by olgagorun on 5/26/15.
*/
public class OffsetStorageFactory {

    public static final String STORAGE_TYPE = "STORAGE_TYPE";
    public static final String CASSANDRA_STORAGE_ADDRESS = "cassandra_storage_addresses";
    public static final String CASSANDRA_STORAGE_KEYSPACE = "cassandra_storage_keyspace";

    public static IOffsetInfoStorage getStorage(Map conf) {
        IOffsetInfoStorage storage = null;
        if (conf.get(STORAGE_TYPE).equals("CASSANDRA")) {
            try {
               storage = new CassandraOffsetInfoStorage(Arrays.asList(( (String)conf.get(CASSANDRA_STORAGE_ADDRESS)).split(",")), (String)conf.get(CASSANDRA_STORAGE_KEYSPACE));
            } catch (UnknownHostException e) {
               throw new RuntimeException("unhandled");
            }
        }
        else {
            storage = new ZkState((String)conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT), conf);
        }

        return storage;
    }
}
