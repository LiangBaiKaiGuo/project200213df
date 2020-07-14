package com.atguigu;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Guo
 * @create 2020-07-13-16:57
 */
public class redis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop102", 6379);

        String ping = jedis.ping();
        Map<String, String> mans = jedis.hgetAll("mans");
        System.out.println(mans);
        System.out.println(ping);

        for (Map.Entry<String, String> map : mans.entrySet()) {
            System.out.println("key:" + map.getKey() + "values:" + map.getValue());
        }
        System.out.println("===================================");
        for (String key : mans.keySet()) {
            System.out.println("key: " + key );
        }
        for (String value : mans.values()) {
            System.out.println("values: " + value);
        }
        Iterator<Map.Entry<String, String>> iterator = mans.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> entry = iterator.next();
            System.out.println("key: " + entry.getKey() + "values: " + entry.getValue());
        }
        jedis.close();
    }
}
