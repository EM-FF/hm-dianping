package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author HH
 * @version 1.0
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){
        // 配置config
        Config config = new Config();
        config.useSingleServer().setAddress("redis://43.142.54.147:6379").setPassword("redis256");
        return Redisson.create(config);
    }
}
