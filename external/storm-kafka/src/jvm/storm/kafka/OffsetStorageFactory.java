package storm.kafka;

import backtype.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
* Created by olgagorun on 5/26/15.
*/
public class OffsetStorageFactory {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    public static final String STORAGE_TYPE = "STORAGE_TYPE";
    public static final String CASSANDRA_STORAGE_ADDRESS = "cassandra_storage_addresses";
    public static final String CASSANDRA_STORAGE_KEYSPACE = "cassandra_storage_keyspace";

    public static IOffsetInfoStorage getStorage(Map conf) {
        IOffsetInfoStorage storage = null;
        if (conf.get(STORAGE_TYPE).equals("CASSANDRA")) {
            LOG.info("cassandra storage chosen");
            try {
               storage = new CassandraOffsetInfoStorage(Arrays.asList(( (String)conf.get(CASSANDRA_STORAGE_ADDRESS)).split(",")), (String)conf.get(CASSANDRA_STORAGE_KEYSPACE));
               LOG.info("cassandra storage created");
            } catch (UnknownHostException e) {
               LOG.error("UnknownHostException while creating storage");
               throw new RuntimeException("unhandled");
            }
        }
        else {
            storage = new ZkState((String)conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT), conf);
            LOG.info("zookeeper storage created");
        }

        return storage;
    }
}
