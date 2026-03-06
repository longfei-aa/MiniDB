package com.minidb.lock;

import java.util.*;

/**
 * 锁对象
 * 表示对某个资源的锁
 */
public class Lock {
    // 锁ID（全局唯一）
    private final long lockId;

    // 行锁类型（仅对行级锁有效）
    private final LockType lockType;

    // 锁定的资源标识（仅行级资源）
    // row:tableId:rowKey / gap:tableId:start-end / nextkey:tableId:rowKey
    private final String resourceId;

    // 持有该锁的事务ID
    private long holderTransactionId;

    // 锁创建时间
    private final long createTime;

    // 间隙锁的范围（仅对GAP和NEXT_KEY锁有效）
    private final Integer gapStart;  // 间隙起始值
    private final Integer gapEnd;    // 间隙结束值

    /**
     * 构造函数（行级锁）
     */
    public Lock(long lockId, LockType lockType,
                String resourceId, Integer gapStart, Integer gapEnd) {
        this.lockId = lockId;
        this.lockType = lockType;
        this.resourceId = resourceId;
        this.holderTransactionId = 0L;
        this.createTime = System.currentTimeMillis();
        this.gapStart = gapStart;
        this.gapEnd = gapEnd;
    }

    public long getLockId() {
        return lockId;
    }

    public LockType getLockType() {
        return lockType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public long getHolderTransactionId() {
        return holderTransactionId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public Integer getGapStart() {
        return gapStart;
    }

    public Integer getGapEnd() {
        return gapEnd;
    }

    /**
     * 添加锁持有者
     */
    public void addHolder(long transactionId) {
        holderTransactionId = transactionId;
    }

    /**
     * 移除锁持有者
     */
    public void removeHolder(long transactionId) {
        if (holderTransactionId == transactionId) {
            holderTransactionId = 0L;
        }
    }

    /**
     * 是否有持有者
     */
    public boolean hasHolders() {
        return holderTransactionId != 0L;
    }

    /**
     * 检查事务是否持有该锁
     */
    public boolean isHeldBy(long transactionId) {
        return holderTransactionId == transactionId;
    }

    /**
     * 获取锁等待时间
     */
    public long getWaitTime() {
        return System.currentTimeMillis() - createTime;
    }

    @Override
    public String toString() {
        String lockInfo = String.format("Lock[id=%d, resource=%s, holder=%d, type=%s",
                lockId, resourceId, holderTransactionId, lockType);
        if (lockType == LockType.GAP || lockType == LockType.NEXT_KEY) {
            lockInfo += String.format(", gap=(%s, %s)", gapStart, gapEnd);
        }
        lockInfo += "]";
        return lockInfo;
    }
}
