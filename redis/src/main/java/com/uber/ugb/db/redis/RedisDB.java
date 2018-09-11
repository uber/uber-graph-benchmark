package com.uber.ugb.db.redis;

import com.uber.ugb.db.KeyValueDB;
import com.uber.ugb.storage.KeyValueStore;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Properties;

public class RedisDB extends KeyValueDB {

    RedisStore redisStore;

    public RedisDB() {
        super();
    }

    @Override
    public void init() {
        redisStore = new RedisStore(getProperties());
        setKeyValueStore(redisStore);
    }

    @Override
    public void cleanup() {
        redisStore.jedisPool.close();
        redisStore.jedisPool.destroy();
    }

    public static class RedisStore implements KeyValueStore {

        final JedisPool jedisPool;

        public RedisStore(Properties properties) {
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            String host = properties.getProperty("redis.host", "localhost");
            int port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
            jedisPool = new JedisPool(config, host, port, 10000);
        }

        @Override
        public byte[] get(byte[] key) {
            Jedis jedis = this.jedisPool.getResource();
            byte[] value = jedis.get(key);
            jedis.close();
            return value;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            Jedis jedis = this.jedisPool.getResource();
            jedis.set(key, value);
            jedis.close();
        }
    }
}
