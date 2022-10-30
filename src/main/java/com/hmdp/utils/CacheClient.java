package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author HH
 * @version 1.0
 */
@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix + id;
        // 1.根据id从redis中查询缓存信息
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.缓存中有，直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否为空值（空字符串）
        if(json != null){
            return null;
        }

        // 3.缓存不存在，到数据库查
        R r = dbFallback.apply(id);
        // 4.数据库没有，返回错误
        if(r == null){
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5.数据库有，将信息存到redis中 设置TTL
        this.set(key,r,time,unit);
        // 6.返回商品信息
        return r;
    }

    public <R,ID> R queryWithLogicalExpire(
            String KeyPrefix, ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = KeyPrefix + id;
        // 1.根据id从redis中查询缓存信息
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.缓存不命中，直接返回
        if(StrUtil.isBlank(json)){
            return null;
        }
        // 3.将json反序列化为bean
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 4.1未过期
            return r;
        }
        // 4.2过期，缓存重建
        // 5.1判断是否获取到锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            // 5.2拿到锁，开启线程访问数据库，存入redis
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 5.3 释放锁
                    unLock(lockKey);
                }
            });
        }


        // 6.返回商品信息
        return r;
    }

    private boolean tryLock(String key){

        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


}
