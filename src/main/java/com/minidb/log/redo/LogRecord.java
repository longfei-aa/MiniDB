package com.minidb.log.redo;

import java.util.Arrays;

/**
 * 日志记录
 * Redo/WAL 的记录模型
 */
public class LogRecord {
    /**
     * 日志类型
     */
    public enum LogType {
        INSERT,     // 插入操作
        UPDATE,     // 更新操作
        DELETE,     // 删除操作
        BEGIN,      // 事务开始
        COMMIT,     // 事务提交
        ABORT,      // 事务回滚
        CHECKPOINT  // Checkpoint
    }

    // 日志序列号（LSN - Log Sequence Number）
    private final long lsn;

    // 日志类型
    private final LogType type;

    // 事务ID
    private final long transactionId;

    // 表ID
    private final int tableId;

    // 页面ID
    private final int pageId;

    // 旧数据（用于Undo）
    private final byte[] oldData;

    // 新数据（用于Redo）
    private final byte[] newData;

    // 前一个日志记录的LSN
    private final long prevLsn;

    // 创建时间
    private final long createTime;

    /**
     * 构造函数
     */
    public LogRecord(long lsn, LogType type, long transactionId, int tableId, int pageId,
                     byte[] oldData, byte[] newData, long prevLsn) {
        this.lsn = lsn;
        this.type = type;
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.pageId = pageId;
        this.oldData = oldData != null ? Arrays.copyOf(oldData, oldData.length) : null;
        this.newData = newData != null ? Arrays.copyOf(newData, newData.length) : null;
        this.prevLsn = prevLsn;
        this.createTime = System.currentTimeMillis();
    }

    public long getLsn() {
        return lsn;
    }

    public LogType getType() {
        return type;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public int getTableId() {
        return tableId;
    }

    public int getPageId() {
        return pageId;
    }

    public byte[] getOldData() {
        return oldData != null ? Arrays.copyOf(oldData, oldData.length) : null;
    }

    public byte[] getNewData() {
        return newData != null ? Arrays.copyOf(newData, newData.length) : null;
    }

    public long getPrevLsn() {
        return prevLsn;
    }

    public long getCreateTime() {
        return createTime;
    }

    @Override
    public String toString() {
        return String.format(
                "LogRecord[lsn=%d, type=%s, txn=%d, table=%d, page=%d, prev=%d]",
                lsn, type, transactionId, tableId, pageId, prevLsn
        );
    }
}
