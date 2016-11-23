package org.apache.storm.cassandra.trident.state;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.storm.cassandra.CassandraContext;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.apache.storm.cassandra.DynamicStatementBuilder.all;
import static org.apache.storm.cassandra.DynamicStatementBuilder.boundQuery;

/**
 * A helper for building a MapState backed by Cassandra. It internalizes some common
 * implementation choices to simplify usage.
 *
 * In the simplest use case, a map state can be constructed with:
 *
 * StateFactory mapState = MapStateFactoryBuilder.opaque()
 *     .withTable("mykeyspace", "year_month_state")
 *     .withKeys("year", "month")
 *     .withJSONBinaryState("state")
 *     .build();
 *
 * for a cassandra table with:
 * mykeyspace.year_month_state {
 *     year: int,
 *     month: int,
 *     state: blob
 * }
 *
 * This will use the storm JSON serializers to convert the state to and from binary format.
 * Other binary serializers can be used with the {@link #withBinaryState(String, Serializer)} method.
 *
 * Storing state in explicit fields (e.g. in a field "sum" of type int) is possible by instead calling
 * {@link #withStateMapper(StateMapper)}. For instance, you can use {@link NonTransactionalTupleStateMapper},
 * {@link TransactionalTupleStateMapper} or {@link OpaqueTupleStateMapper} if your state values are tuples.
 *
 */
public class MapStateFactoryBuilder<T> {

    private static final Logger logger = LoggerFactory.getLogger(MapStateFactoryBuilder.class);

    private String keyspace;
    private String table;
    private String[] keys;
    private Integer maxParallelism;
    private StateType stateType;
    private StateMapper<T> stateMapper;
    private Map cassandraConfig;
    private int cacheSize;

    public static <U> MapStateFactoryBuilder<OpaqueValue<U>> opaque(Map cassandraConf) {
        return new MapStateFactoryBuilder<OpaqueValue<U>>()
                .withStateType(StateType.OPAQUE)
                .withCassandraConfig(cassandraConf);
    }

    public static <U> MapStateFactoryBuilder<TransactionalValue<U>> transactional(Map cassandraConf) {
        return new MapStateFactoryBuilder<TransactionalValue<U>>()
                .withStateType(StateType.TRANSACTIONAL)
                .withCassandraConfig(cassandraConf);
    }

    public static <U> MapStateFactoryBuilder<U> nontransactional(Map cassandraConf) {
        return new MapStateFactoryBuilder<U>()
                .withStateType(StateType.NON_TRANSACTIONAL)
                .withCassandraConfig(cassandraConf);
    }

    public MapStateFactoryBuilder<T> withTable(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
        return this;
    }

    public MapStateFactoryBuilder<T> withKeys(String... keys) {
        this.keys = keys;
        return this;
    }

    public MapStateFactoryBuilder<T> withMaxParallelism(Integer maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    @SuppressWarnings("unchecked")
    public MapStateFactoryBuilder<T> withJSONBinaryState(String stateField) {
        switch (stateType) {
            case OPAQUE:
                return withBinaryState(stateField, (Serializer) new JSONOpaqueSerializer());
            case TRANSACTIONAL:
                return withBinaryState(stateField, (Serializer) new JSONTransactionalSerializer());
            case NON_TRANSACTIONAL:
                return withBinaryState(stateField, new JSONNonTransactionalSerializer());
            default:
                throw new IllegalArgumentException("State type " + stateType + " is unknown.");
        }
    }

    public MapStateFactoryBuilder<T> withStateMapper(StateMapper<T> stateMapper) {
        this.stateMapper = stateMapper;
        return this;
    }

    public MapStateFactoryBuilder<T> withBinaryState(String stateField, Serializer<T> serializer) {
        return withStateMapper(new SerializedStateMapper<>(stateField, serializer));
    }

    protected MapStateFactoryBuilder<T> withStateType(StateType stateType) {
        this.stateType = stateType;
        return this;
    }

    protected MapStateFactoryBuilder<T> withCassandraConfig(Map cassandraConf) {
        this.cassandraConfig = cassandraConf;
        return this;
    }

    public MapStateFactoryBuilder<T> withCache(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public StateFactory build() {

        Objects.requireNonNull(keyspace, "A keyspace is required.");
        Objects.requireNonNull(table, "A table name is required.");
        Objects.requireNonNull(keys, "At least one key must be specified.");
        if (keys.length == 0) {
            throw new IllegalArgumentException("At least one key must be specified.");
        }
        Objects.requireNonNull(stateMapper, "A state mapper must be specified.");
        Objects.requireNonNull(stateType, "A state type must be specified.");

        List<String> stateFields = stateMapper.getStateFields()
                .toList();

        String[] stateFieldsArray = stateFields.toArray(new String[stateFields.size()]);

        List<String> allFields = new ArrayList<>();
        Collections.addAll(allFields, keys);
        allFields.addAll(stateFields);

        // Build get query
        Select.Where getQuery = select(stateFieldsArray)
                .from(keyspace, table)
                .where();

        for (String key : keys) {
            getQuery.and(eq(key, bindMarker()));
        }

        CQLStatementTupleMapper get = boundQuery(getQuery.toString())
                .bind(all())
                .build();

        // Build put query
        Insert putStatement = insertInto(keyspace, table)
                .values(allFields, Collections.<Object>nCopies(allFields.size(), bindMarker()));

        CQLStatementTupleMapper put = boundQuery(putStatement.toString())
                .bind(all())
                .build();

        CassandraBackingMap.Options options = new CassandraBackingMap.Options<T>(new CassandraContext())
                .withGetMapper(get)
                .withPutMapper(put)
                .withStateMapper(stateMapper)
                .withKeys(new Fields(keys))
                .withMaxParallelism(maxParallelism);

        logger.debug("Building factory with: \n  get: {}\n  put: {}\n  mapper: {}",
                getQuery.toString(),
                putStatement.toString(),
                stateMapper.toString());

        switch (stateType) {
            case NON_TRANSACTIONAL:
                return CassandraMapStateFactory.nonTransactional(options, cassandraConfig)
                        .withCache(cacheSize);
            case TRANSACTIONAL:
                return CassandraMapStateFactory.transactional(options, cassandraConfig)
                        .withCache(cacheSize);
            case OPAQUE:
                return CassandraMapStateFactory.opaque(options, cassandraConfig)
                        .withCache(cacheSize);
        }

        return null;
    }

}
