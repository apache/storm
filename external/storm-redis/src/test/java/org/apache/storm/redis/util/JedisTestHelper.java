package org.apache.storm.redis.util;

import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import java.util.Objects;

/**
 * Utility class for helping interact with a Redis service in tests.
 */
public class JedisTestHelper {
    private final Jedis jedis;

    /**
     * Constructor.
     * @param container Container instance to create a redis client against.
     */
    public JedisTestHelper(final GenericContainer container) {
        Objects.requireNonNull(container);

        jedis = new Jedis(
            container.getHost(),
            container.getFirstMappedPort()
        );
    }

    public void delete(final String key) {
        jedis.del(key);
    }

    public void geoadd(final String key, final double longitude, final double latitude, final String value) {
        jedis.geoadd(key, longitude, latitude, value);
    }

    public boolean hexists(final String hash, final String key) {
        return jedis.hexists(hash, key);
    }

    public void hset(final String hash, final String key, final String value) {
        jedis.hset(hash, key, value);
    }

    public boolean exists(final String key) {
        return jedis.exists(key);
    }

    public void pfadd(final String key, final String value) {
        jedis.pfadd(key, value);
    }

    public void set(final String key, final String value) {
        jedis.set(key, value);
    }

    public void smember(final String set, final String value) {
        jedis.sadd(set, value);
    }

    public boolean sismember(final String set, final String value) {
        return jedis.sismember(set, value);
    }

    public void zrank(final String set, final double score, final String value) {
        jedis.zadd(set, score, value);
    }

    public void close() {
        jedis.close();
    }
}
