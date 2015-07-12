package com.dianping.cosmos;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RedisSinkBolt implements IRichBolt {
    private final Log LOG = LogFactory.getLog(RedisSinkBolt.class);
    private OutputCollector collector;
    private JedisPool pool;
    private Updater updater;
    
    private String redisHost;
    private int redisPort;
    private int timeout;
    private int retryLimit;
    
    public RedisSinkBolt(String redisHost, int redisPort) {
        this(redisHost, redisPort, 50, 3);
    }
    
    public RedisSinkBolt(String redisHost, int redisPort, int retryLimit) {
        this(redisHost, redisPort, 50, retryLimit);
    }
    
    public RedisSinkBolt(String redisHost, int redisPort, int timeout, int retryLimit) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.timeout = timeout;
        this.retryLimit = retryLimit;
    }
    
    public void setUpdater(Updater updater) {
        this.updater = updater;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
        GenericObjectPoolConfig pconf = new GenericObjectPoolConfig();
        pconf.setMaxWaitMillis(2000);
        pconf.setMaxTotal(1000);
        pconf.setTestOnBorrow(false);
        pconf.setTestOnReturn(false);
        pconf.setTestWhileIdle(true);
        pconf.setMinEvictableIdleTimeMillis(120000);
        pconf.setTimeBetweenEvictionRunsMillis(60000);
        pconf.setNumTestsPerEvictionRun(-1);
        
        pool = new JedisPool(pconf, redisHost, redisPort, timeout);
    }

    private byte[] retryGet(byte[] key) {
        int retry = 0;
        byte[] ret;
        while (true) {
            Jedis jedis = null;
            try {
                jedis = pool.getResource();
                ret = jedis.get(key);
                return ret;
            } catch (JedisConnectionException e) {
                if (jedis != null) {
                    pool.returnBrokenResource(jedis);
                    jedis = null;
                }
                if (retry > retryLimit) {
                    throw e;
                }
                retry++;
            } finally {
                if (jedis != null) {
                    pool.returnResource(jedis);
                }
            }
        }
    }
    
    private String retrySet(byte[] key, byte[] value) {
        int retry = 0;
        String ret;
        while (true) {
            Jedis jedis = null;
            try {
                jedis = pool.getResource();
                ret = jedis.set(key, value);
                return ret;
            } catch (JedisConnectionException e) {
                if (jedis != null) {
                    pool.returnBrokenResource(jedis);
                    jedis = null;
                }
                if (retry > retryLimit) {
                    throw e;
                }
                retry++;
            } finally {
                if (jedis != null) {
                    pool.returnResource(jedis);
                }
            }
            
        }
    }
    
    @Override
    public void execute(Tuple input) {
        byte[] key = input.getBinary(0);
        byte[] value = input.getBinary(1);
        
        if (key == null || value == null) {
            collector.ack(input);
            return;
        }
        
        try {
            if (updater != null) {
                byte[] oldValue = retryGet(key);
                byte[] newValue = updater.update(oldValue, value);
                if (newValue == null) {
                    collector.ack(input);
                    return;
                }
                retrySet(key, newValue);
                collector.ack(input);
                return;
            }
            
            retrySet(key, value);
            collector.ack(input);
        } catch (JedisConnectionException e) {
            LOG.warn("JedisConnectionException catched ", e);
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        pool.destroy();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
