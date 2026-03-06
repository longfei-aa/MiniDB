package com.minidb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务管理器
 * 负责事务的创建、提交、回滚、活跃事务表维护和一致快照生成
 */
public class TransactionManager {
    // 全局事务ID生成器（从1开始，0保留给系统）
    private final AtomicLong nextTransactionId;

    // 活跃事务表：transactionId -> Transaction
    private final ConcurrentHashMap<Long, Transaction> activeTransactions;

    // 默认隔离级别
    private IsolationLevel defaultIsolationLevel;

    // 统计信息
    private final AtomicLong totalCommitted;
    private final AtomicLong totalAborted;

    /**
     * 活跃事务快照。
     * 用于一次性捕获 ReadView 需要的 activeIds / minActive / nextTxnId，
     * 避免分多次读取时得到不一致的中间态。
     */
    public static final class ActiveTransactionSnapshot {
        private final List<Long> activeTransactionIds;
        private final long minActiveTransactionId;
        private final long nextTransactionId;

        public ActiveTransactionSnapshot(List<Long> activeTransactionIds,
                                         long minActiveTransactionId,
                                         long nextTransactionId) {
            this.activeTransactionIds = activeTransactionIds;
            this.minActiveTransactionId = minActiveTransactionId;
            this.nextTransactionId = nextTransactionId;
        }

        public List<Long> getActiveTransactionIds() {
            return new ArrayList<>(activeTransactionIds);
        }

        public long getMinActiveTransactionId() {
            return minActiveTransactionId;
        }

        public long getNextTransactionId() {
            return nextTransactionId;
        }
    }

    /**
     * 构造函数
     */
    public TransactionManager() {
        this.nextTransactionId = new AtomicLong(1);
        this.activeTransactions = new ConcurrentHashMap<>();
        this.defaultIsolationLevel = IsolationLevel.REPEATABLE_READ;
        this.totalCommitted = new AtomicLong(0);
        this.totalAborted = new AtomicLong(0);
    }

    /**
     * 开始一个新事务（使用默认隔离级别）
     */
    public Transaction beginTransaction() {
        return beginTransaction(defaultIsolationLevel);
    }

    /**
     * 开始一个新事务（指定隔离级别）
     */
    public Transaction beginTransaction(IsolationLevel isolationLevel) {
        long txnId = nextTransactionId.getAndIncrement();
        Transaction txn = new Transaction(txnId, isolationLevel);
        activeTransactions.put(txnId, txn);

        return txn;
    }

    /**
     * 提交事务
     */
    public void commit(Transaction txn) {
        if (txn == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }

        if (!txn.isActive()) {
            throw new IllegalStateException("Transaction is not active: " + txn.getState());
        }

        // 设置为准备提交状态
        txn.setState(TransactionState.PREPARING);

        try {
            // 注意：Redo Log 的写入由 Executor 层负责
            // 在 Executor.executeCommit() 中已经调用了：
            // - redoLog.writeCommitLog(txn.getTransactionId())
            // - bufferPool.flushAllPages()
            // TransactionManager 只负责事务状态管理

            // 标记为已提交
            txn.setState(TransactionState.COMMITTED);

            // 从活跃事务表移除
            activeTransactions.remove(txn.getTransactionId());

            // 更新统计
            totalCommitted.incrementAndGet();

        } catch (Exception e) {
            // 提交失败，回滚
            txn.setState(TransactionState.ABORTED);
            throw new RuntimeException("Failed to commit transaction: " + txn.getTransactionId(), e);
        }
    }

    /**
     * 回滚事务
     */
    public void rollback(Transaction txn) {
        if (txn == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }

        if (txn.isAborted()) {
            // 已回滚，直接返回
            return;
        }
        if (txn.isCommitted()) {
            throw new IllegalStateException("Cannot rollback committed transaction: " + txn.getTransactionId());
        }
        if (!txn.isActive()) {
            throw new IllegalStateException("Transaction is not active: " + txn.getState());
        }

        try {
            // 注意：Undo Log 的回滚由 Executor 层负责
            // 在 Executor.executeRollback() 中已经调用了：
            // - mvccManager.rollback(currentTransaction)
            // - redoLog.writeAbortLog(txn.getTransactionId())
            // TransactionManager 只负责事务状态管理

            // 标记为已回滚
            txn.setState(TransactionState.ABORTED);

            // 从活跃事务表移除
            activeTransactions.remove(txn.getTransactionId());

            // 更新统计
            totalAborted.incrementAndGet();

        } catch (Exception e) {
            throw new RuntimeException("Failed to rollback transaction: " + txn.getTransactionId(), e);
        }
    }

    /**
     * 获取当前所有活跃事务ID列表（用于构建ReadView）
     */
    public List<Long> getActiveTransactionIds() {
        return captureSnapshot().getActiveTransactionIds();
    }

    /**
     * 获取最小活跃事务ID
     */
    public long getMinActiveTransactionId() {
        return captureSnapshot().getMinActiveTransactionId();
    }

    /**
     * 一次性捕获活跃事务快照，供 ReadView 和 purge 低水位计算使用。
     */
    public ActiveTransactionSnapshot captureSnapshot() {
        List<Long> activeIds = new ArrayList<>(activeTransactions.keySet());
        long nextTxnId = nextTransactionId.get();
        long minActiveTxnId = activeIds.stream()
            .min(Long::compareTo)
            .orElse(nextTxnId);
        return new ActiveTransactionSnapshot(activeIds, minActiveTxnId, nextTxnId);
    }

    /**
     * 获取下一个待分配的事务ID
     */
    public long getNextTransactionId() {
        return nextTransactionId.get();
    }

    /**
     * 启动恢复后用于推进 nextTransactionId，避免新事务ID回退到已落盘版本之前。
     */
    public long ensureNextTransactionIdAtLeast(long candidateNextTransactionId) {
        if (candidateNextTransactionId <= 1) {
            return nextTransactionId.get();
        }
        return nextTransactionId.updateAndGet(current -> Math.max(current, candidateNextTransactionId));
    }

    /**
     * 获取活跃事务数量
     */
    public int getActiveTransactionCount() {
        return activeTransactions.size();
    }

    /**
     * 获取指定事务
     */
    public Transaction getTransaction(long transactionId) {
        return activeTransactions.get(transactionId);
    }

    /**
     * 设置默认隔离级别
     */
    public void setDefaultIsolationLevel(IsolationLevel level) {
        this.defaultIsolationLevel = level;
    }

    /**
     * 获取默认隔离级别
     */
    public IsolationLevel getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format(
                "TransactionManager Stats:\n" +
                "  Active Transactions: %d\n" +
                "  Total Committed: %d\n" +
                "  Total Aborted: %d\n" +
                "  Next Transaction ID: %d\n" +
                "  Default Isolation Level: %s",
                activeTransactions.size(),
                totalCommitted.get(),
                totalAborted.get(),
                nextTransactionId.get(),
                defaultIsolationLevel
        );
    }

    /**
     * 清理所有事务（用于测试和关闭）
     */
    public void shutdown() {
        // 基于快照遍历，避免遍历过程中 remove 当前 map 项造成阅读歧义
        for (Transaction txn : new ArrayList<>(activeTransactions.values())) {
            if (txn.isActive()) {
                try {
                    rollback(txn);
                } catch (Exception e) {
                    System.err.println("Failed to rollback transaction during shutdown: " + txn.getTransactionId());
                }
            }
        }
        activeTransactions.clear();
    }
}
