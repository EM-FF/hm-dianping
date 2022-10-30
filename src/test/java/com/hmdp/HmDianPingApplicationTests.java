package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testSaveShop(){
        shopService.saveShop2Redis(1L,30L);
    }

    @Test
     void testIdWork(){
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.nextId("order");
                System.out.println("order = " + order);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println(" 用时：" + (end - begin));
    }

    @Test
     void loadData(){
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.根据typeId分组
        Map<Long,List<Shop>> map = list.stream().
                collect(Collectors.groupingBy(Shop::getTypeId));

        // 3.分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1获取类型id
            Long id = entry.getKey();
            String key = SHOP_GEO_KEY + id;
            // 3.2获取同类型商家集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3写入redis GEOADD key 经度 维度 member
            for (Shop shop : value) {
                locations.add(
                        new RedisGeoCommands.GeoLocation<String>(
                                shop.getId().toString(),
                                new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void testHyperLogLog(){
        String[] values = new String[1000];
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if(j == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hll",values);
            }

        }
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll");
        System.out.println(size);
    }
}
