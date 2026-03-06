package com.minidb.transaction;

/**
 * 事务对象
 * 表示一个数据库事务，包含事务ID、状态、隔离级别等信息
 */
public class Transaction {
    // 事务ID（全局唯一）
    private final long transactionId;

    // 事务状态
    private volatile TransactionState state;

    // 隔离级别
    private final IsolationLevel isolationLevel;

    // 事务开始时间
    private final long startTime;

    /**
     * 构造函数
     */
    public Transaction(long transactionId, IsolationLevel isolationLevel) {
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.state = TransactionState.ACTIVE;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 获取事务ID
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * 获取事务状态
     */
    public TransactionState getState() {
        return state;
    }

    /**
     * 设置事务状态
     */
    public void setState(TransactionState state) {
        this.state = state;
    }

    /**
     * 获取隔离级别
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * 获取事务开始时间
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * 判断事务是否活跃
     */
    public boolean isActive() {
        return state == TransactionState.ACTIVE;
    }

    /**
     * 判断事务是否已提交
     */
    public boolean isCommitted() {
        return state == TransactionState.COMMITTED;
    }

    /**
     * 判断事务是否已回滚
     */
    public boolean isAborted() {
        return state == TransactionState.ABORTED;
    }

    @Override
    public String toString() {
        return String.format("Transaction[id=%d, state=%s, isolation=%s]",
                transactionId, state, isolationLevel);
    }
}
