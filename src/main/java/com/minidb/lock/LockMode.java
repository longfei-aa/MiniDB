package com.minidb.lock;

/**
 * 锁模式
 * 面试版仅保留排他锁（当前读路径全部使用X锁）
 */
public enum LockMode {
    X
}
