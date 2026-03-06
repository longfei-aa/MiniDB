package com.minidb.lock;

/**
 * 行锁类型
 * 用于实现可重复读隔离级别，防止幻读
 */
public enum LockType {
    /**
     * 记录锁（Record Lock）
     * 锁定索引记录，精确锁定单行
     */
    RECORD,

    /**
     * 间隙锁（Gap Lock）
     * 锁定索引记录之间的间隙，防止插入
     * 例如：锁定(10, 20)之间的间隙，防止插入15
     */
    GAP,

    /**
     * 临键锁（Next-Key Lock）
     * Record Lock + Gap Lock
     * 锁定记录及其之前的间隙
     * 例如：锁定20及其之前的间隙(10, 20]
     * 这是InnoDB在REPEATABLE READ下的默认锁
     */
    NEXT_KEY
}
