package org.springframework.data.redis.util;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import redis.clients.jedis.*;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * scan redis keys, support cluster and Standalone
 */
public class RedisScanUtil {

    public static <K, V> void scanMatch(RedisTemplate<K, V> redisTemplate, String matchKey, ScanKeysIterator keysIterator) {
        Assert.notNull(redisTemplate, "redisTemplate is null");
        Assert.notNull(keysIterator, "keysIterator is null");
        Assert.hasText(matchKey, "matchKey is blank");

        RedisConnectionFactory connectionFactory = redisTemplate.getConnectionFactory();
        if (connectionFactory == null) {
            throw new IllegalStateException("redisTemplate.getConnectionFactory() is null");
        }

        try (RedisConnection redisConnection = connectionFactory.getConnection();) {
            if (redisConnection instanceof JedisConnection || redisConnection instanceof RedisSentinelConnection) {
                Cursor<byte[]> scan = redisConnection.scan(ScanOptions.scanOptions().match(matchKey).count(10 * 10000).build());
                while (scan.hasNext()) {
                    keysIterator.iterator(new String(scan.next()));
                }
            }
//            else if (redisConnection instanceof RedisSentinelConnection) {
//                scanMatchCluster(redisTemplate, matchKey, keysIterator);
//            }
            else if (redisConnection instanceof RedisClusterConnection) {
                scanMatchCluster(redisTemplate, matchKey, keysIterator);
            }
        }
    }


    private static <K, V> void scanMatchCluster(RedisTemplate<K, V> redisTemplate, String key, ScanKeysIterator keysIterator) {
        redisTemplate.execute((RedisCallback<Object>) redisConnection -> {
            JedisCluster cluster = (JedisCluster) redisConnection.getNativeConnection();
            Collection<JedisPool> pools = cluster.getClusterNodes().values();
            pools.stream().filter(Objects::nonNull).forEach(jedisPool -> {
                try (Jedis resource = jedisPool.getResource();) {
                    ScanParams scanParams = new ScanParams();
                    scanParams.match(key);
                    scanParams.count(10 * 10000);
                    ScanResult<String> scan = resource.scan("0", scanParams);
                    while (null != scan.getCursor()) {
                        List<String> list = scan.getResult();

                        if (CollectionUtils.isNotEmpty(list)) {
                            for (String s : list) {
                                keysIterator.iterator(s);
                            }
                        }

                        if ("0".equals(scan.getCursor())) {
                            break;
                        }
                        scan = resource.scan(scan.getCursor(), scanParams);
                    }
                }
            });
            return null;
        });
    }

    public interface ScanKeysIterator {

        void iterator(String key);
    }
}
