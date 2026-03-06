package com.minidb.transaction;

/**
 * 事务状态
 */
public enum TransactionState {
    /**
     * 活跃状态：事务正在执行
     */
    ACTIVE,

    /**
     * 准备提交：写入日志但未提交
     */
    PREPARING,

    /**
     * 已提交
     */
    COMMITTED,

    /**
     * 已回滚
     */
    ABORTED
}
