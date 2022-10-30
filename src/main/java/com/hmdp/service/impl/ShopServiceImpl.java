package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient.
//                queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.
                queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1.根据id从redis中查询缓存信息
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.缓存不命中，直接返回
//        if(StrUtil.isBlank(shopJson)){
//            return null;
//        }
//        // 3.将json反序列化为bean
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject data = (JSONObject)redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 4.判断缓存是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            // 4.1未过期
//            return shop;
//        }
//        // 4.2过期，缓存重建
//        // 5.1判断是否获取到锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        if(isLock){
//            // 5.2拿到锁，开启线程访问数据库，存入redis
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    // 5.3 释放锁
//                    unLock(lockKey);
//                }
//            });
//        }
//
//
//        // 6.返回商品信息
//        return shop;
//    }

//    public Shop queryWithMutex(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1.根据id从redis中查询缓存信息
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.缓存中有，直接返回
//        if(StrUtil.isNotBlank(shopJson)){
//            return JSONUtil.toBean(shopJson,Shop.class);
//        }
//        String lockKey = null;
//        Shop shop = null;
//        try {
//            // 3.判断命中的是否为空值（空字符串）
//            if(shopJson != null){
//                return null;
//            }
//
//            // 4.缓存重建
//            // 4.1尝试获取互斥锁
//            lockKey = LOCK_SHOP_KEY + id;
//            boolean isLock = tryLock(lockKey);
//            // 4.2判断是否获取到锁
//            if(!isLock){
//                //  4.3没有，休眠
//                Thread.sleep(50);
//            }
//
//            // 4.4.缓存不存在，到数据库查
//            shop = getById(id);
//            // 5.数据库没有，返回错误
//            if(shop == null){
//                // 将空值写入redis
//                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            // 6.数据库有，将信息存到redis中 设置TTL
//            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//            // 7.释放锁
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }finally {
//            unLock(lockKey);
//        }
//        // 8.返回商品信息
//        return shop;
//    }

//    public Shop queryWithPassThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1.根据id从redis中查询缓存信息
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.缓存中有，直接返回
//        if(StrUtil.isNotBlank(shopJson)){
//            return JSONUtil.toBean(shopJson,Shop.class);
//        }
//        // 判断命中的是否为空值（空字符串）
//        if(shopJson != null){
//            return null;
//        }
//
//        // 3.缓存不存在，到数据库查
//        Shop shop = getById(id);
//        // 4.数据库没有，返回错误
//        if(shop == null){
//            // 将空值写入redis
//            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        // 5.数据库有，将信息存到redis中 设置TTL
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        // 6.返回商品信息
//        return shop;
//    }
//
//    private boolean tryLock(String key){
//
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//    private void unLock(String key){
//        stringRedisTemplate.delete(key);
//    }

    public void saveShop2Redis(Long id, Long expireSeconds){
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 2.封装过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    @Transactional
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店家id不能为空");
        }
        // 1.先修改数据库
        updateById(shop);
        // 2.再修改redis

        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis 按照距离排序
        String key = SHOP_GEO_KEY + typeId;
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                        .includeDistance().limit(end)

        );

        // 4.解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(end-from);
        Map<String,Distance> distanceMap = new HashMap<>(end-from);
        list.stream().skip(from).forEach(result -> {
            // 4.1 获取id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.2获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        // 5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")")
                .list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
