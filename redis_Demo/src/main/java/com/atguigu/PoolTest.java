package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * @author Guo
 * @create 2020-07-13-18:06
 */
public class PoolTest {
    public static void main(String[] args) {
        String host = "hadoop102";

        int port = Protocol.DEFAULT_PORT;

        JedisPool jedisPool = new JedisPool(host, port);

        Jedis resource = jedisPool.getResource();

        String ping = resource.ping();
        String mans = resource.get("str");
        String hget = resource.hget("mans", "name");

        System.out.println(hget);
        System.out.println(mans);
        System.out.println(ping);

        System.out.println("git hotfix test");

        jedisPool.close();
    }
}
