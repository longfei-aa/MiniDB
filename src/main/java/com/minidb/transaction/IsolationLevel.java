package com.minidb.transaction;

/**
 * 事务隔离级别
 */
public enum IsolationLevel {
    /**
     * 读未提交（不实现，仅作占位）
     */
    READ_UNCOMMITTED,

    /**
     * 读已提交：每次SELECT都生成新的ReadView
     */
    READ_COMMITTED,

    /**
     * 可重复读（默认级别）：事务开始时生成ReadView，整个事务期间使用同一个ReadView
     */
    REPEATABLE_READ,

    /**
     * 串行化（不实现，仅作占位）
     */
    SERIALIZABLE
}