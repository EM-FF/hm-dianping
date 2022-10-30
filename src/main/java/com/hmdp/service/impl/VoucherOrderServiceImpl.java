package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private SeckillVoucherServiceImpl seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private IVoucherOrderService proxy;

    // 线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 提交线程
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    // 开启线程，提交订单
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    // 1.获取消息队列中订单 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 stream:order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2L)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断是否获取成功
                    if(list == null || list.isEmpty()){
                        // 2.1失败，继续下次循环
                        continue;
                    }
                    // 3.解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.成功，创建订单
                    handlerVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream:order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    // 1.获取消息队列中订单 XREADGROUP GROUP g1 c1 COUNT 1  stream:order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断是否获取成功
                    if(list == null || list.isEmpty()){
                        // 2.1失败，说明pending里没有消息了，结束循环
                        break;
                    }
                    // 3.解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.成功，创建订单
                    handlerVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream:order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

    }
//    // 开启线程，提交订单
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    // 1.获取阻塞队列中订单
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 2.创建订单
//                    handlerVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }


    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户id
        Long userId = voucherOrder.getId();
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 2.获取锁
        boolean isLock = redisLock.tryLock();
        // 3.判断是否获得锁
        if(!isLock){
            log.info("一人只能下单一次");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.执行lua脚本
        Long userId = UserHolder.getUser().getId();
        // 2.3订单id
        long orderId = redisIdWorker.nextId("order");
        // 2.判断是否为0
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.1 不为0，没有购买资格
        if(r != 0){
            return Result.fail(r == 1? "库存不足":"不能重复下单");
        }
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4.返回成功信息
        return Result.ok(orderId);
    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.执行lua脚本
//        Long userId = UserHolder.getUser().getId();
//        // 2.判断是否为0
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//        // 2.1 不为0，没有购买资格
//        if(r != 0){
//            return Result.fail(r == 1? "库存不足":"不能重复下单");
//        }
//        // 2.2 0，有购买资格，把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 2.3订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 2.4设置用户id
//        voucherOrder.setUserId(userId);
//        // 2.5设置代金券id
//        voucherOrder.setVoucherId(voucherId);
//
//        // 3.获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 2.6放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//
//
//        // 4.返回成功信息
//        return Result.ok(orderId);
//    }

    //        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            // 否，返回异常
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3.判断秒杀是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            // 否，返回异常
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if(voucher.getStock() <= 0){
//            // 不足，返回异常
//            return Result.fail("库存不足");
//        }
//
//        // 提交完事务，再释放锁
//        Long userId = UserHolder.getUser().getId();
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        // 获取锁
//        boolean isLock = lock.tryLock();
//        // 判断是否获得锁
//        if(!isLock){
//            return Result.fail("一人只能下单一次");
//        }
//        try {
//            // 获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            // 释放锁
//            lock.unlock();
//        }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        // 5.一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 5.1 查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2判断是否存在
        if(count > 0){
            log.error("用户已经购买过一次了");
        }
        // 5.是，库存-1
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("Voucher_id", voucherId).gt("stock",0)//stock >0
                .update();
        if(!success){
            log.error("库存不足");
        }
        save(voucherOrder);

    }
}
