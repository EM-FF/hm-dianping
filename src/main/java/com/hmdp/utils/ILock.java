package com.hmdp.utils;

/**
 * @author HH
 * @version 1.0
 */
public interface ILock {

    /**
     * 获取锁
     * @param timeoutSec 超时时间
     * @return true代表获取锁成功 false获取锁失败
     */
    boolean tryLock(long timeoutSec);

    /**
     释放锁
     */
    void unLock();
}
