/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.uber.ugb.db.redis;

import com.uber.ugb.db.KeyValueDB;
import com.uber.ugb.storage.KeyValueStore;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Properties;

public class RedisDB extends KeyValueDB {

    transient RedisStore redisStore;

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
