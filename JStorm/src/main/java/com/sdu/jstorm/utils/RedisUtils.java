package com.sdu.jstorm.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis Utils
 *
 * @author hanhan.zhang
 * */
public class RedisUtils {

    private static JedisPool JEDISPOOL;

    static {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxIdle(10);
        conf.setTestOnBorrow(true);
        JEDISPOOL = new JedisPool(conf, "localhost");
    }

    public static Jedis getRedis() {
        return JEDISPOOL.getResource();
    }

    public static void recycleRedis(Jedis redis) {
        redis.close();
    }
}
