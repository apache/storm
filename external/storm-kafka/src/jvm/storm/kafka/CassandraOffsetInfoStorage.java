package storm.kafka;

import com.datastax.driver.core.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by olgagorun on 5/25/15.
 */
public class CassandraOffsetInfoStorage implements IOffsetInfoStorage {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraOffsetInfoStorage.class);
    final private String TABLE = "storm_kafka_offsets";

    private Session session;

    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    CassandraOffsetInfoStorage(List<String> addressList, String keyspace) throws UnknownHostException {
        LOG.info("provided addresses:" + addressList.toString());
        List<InetAddress> inetAddresses = new ArrayList<InetAddress>();
        for (String item : addressList) {
            inetAddresses.add(InetAddress.getByName(item));
        }
        Cluster cluster = Cluster.builder().addContactPoints(inetAddresses).build();
        session = cluster.connect(keyspace);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " \n" +
                "( spoutid text, partitionid text, data text,\n" +
                " PRIMARY KEY ((spoutid), partitionid))"
               );
        LOG.info("table " + TABLE + " created");

        insertStatement = session.prepare("INSERT INTO " + TABLE + " (spoutId, partitionId, data) VALUES (?, ?, ?)");
        selectStatement = session.prepare("SELECT * FROM " + TABLE + " WHERE spoutId = ? AND partitionId = ?");
    }

    @Override
    public void set(String spoutId, String partitionId, Map<Object, Object> data) {
        BoundStatement boundStatement = new BoundStatement(insertStatement);
        session.execute(boundStatement.bind(spoutId, partitionId, JSONValue.toJSONString(data)));
    }

    @Override
    public Map<Object, Object> get(String spoutId, String partitionId) {
        BoundStatement boundStatement = new BoundStatement(selectStatement);
        Row row = session.execute(boundStatement.bind(spoutId, partitionId)).one();
        return row == null ? null : (Map<Object,Object>)JSONValue.parse(row.getString("data"));
    }

    public void close() {
        session.close();
    }

}
