package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

/**
 * @author HH
 * @version 1.0
 */
@SpringBootTest
@Slf4j
public class RedissonTest {

    @Resource
    private RedissonClient redissonClient;

    private RLock lock;
    @BeforeEach
    void setUp(){
        lock = redissonClient.getLock("order");
    }

    @Test
    void method1(){
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败");
            return;
        }try {
            log.info("获取锁成功");
            method2();
            log.info("执行工作");
        }finally {
            log.warn("准备释放锁");
            lock.unlock();
        }



    }
    void method2(){
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败");
            return;
        }try {
            log.info("获取锁成功");

            log.info("执行工作");
        }finally {
            log.warn("准备释放锁");
            lock.unlock();
        }
    }


}
