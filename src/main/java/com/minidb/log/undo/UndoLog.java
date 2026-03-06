package com.minidb.log.undo;

import com.minidb.transaction.Transaction;
import com.minidb.common.DataRecord;
import com.minidb.common.RecordSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UndoLog - 内存版历史版本存储与回滚链管理器
 *
 * 职责：
 * - 为事务追加历史版本记录（VersionRecord）
 * - 维护两条链路：
 *   1) 同一行版本链（prevRollPtr）
 *   2) 同一事务回滚链（prevTxnVersionPtr）
 * - 在事务回滚/语句级回滚时按链路逆序应用
 *
 * 说明：
 * - 这里的 Undo 不单是“日志”，更像内存版旧版本仓库
 * - 旧版本仅驻留内存，不做独立持久化
 * - 崩溃恢复由 RedoLog 负责
 */
public class UndoLog {
    /**
     * 历史版本的操作类型。
     * 这里只保留 MVCC/回滚真正需要的三种行级变更，不复用 redo 的日志类型枚举。
     */
    public enum VersionType {
        INSERT,
        UPDATE,
        DELETE
    }

    // 事务的最新版本指针：transactionId -> latest version ptr（用于回滚链）
    private final ConcurrentHashMap<Long, Long> txnLatestVersionPtr;

    // version_ptr -> VersionRecord
    private final ConcurrentHashMap<Long, VersionRecord> versionRecords;

    // 单调递增的内存版本指针
    private final AtomicLong nextVersionPtr;

    // 统计信息
    private final AtomicLong totalVersionRecords;
    private final AtomicLong totalRollbacks;

    // 已提交事务集合。purge 仅能回收这些事务产生的历史版本。
    private final ConcurrentHashMap<Long, Boolean> committedTransactions;

    // Undo 应用器（用于执行回滚操作）
    private UndoApplier undoApplier;

    /**
     * 单条历史版本记录。
     * 它既服务于 MVCC 版本链，也服务于事务回滚链。
     */
    public static class VersionRecord {
        private final long versionPtr;
        private final long transactionId;
        private final VersionType operationType;
        private final String rowKey;
        private final byte[] oldData;
        private final long prevRollPtr;     // 同一行的前一个版本 roll_ptr（行版本链）
        private final long prevTxnVersionPtr;  // 同一事务的前一条 version ptr（事务回滚链）
        private final long createTime;

        public VersionRecord(long versionPtr, long transactionId, VersionType operationType,
                             String rowKey, byte[] oldData, long prevRollPtr, long prevTxnVersionPtr) {
            this.versionPtr = versionPtr;
            this.transactionId = transactionId;
            this.operationType = operationType;
            this.rowKey = rowKey;
            this.oldData = oldData;
            this.prevRollPtr = prevRollPtr;
            this.prevTxnVersionPtr = prevTxnVersionPtr;
            this.createTime = System.currentTimeMillis();
        }

        public long getVersionPtr() {
            return versionPtr;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public VersionType getOperationType() {
            return operationType;
        }

        public String getRowKey() {
            return rowKey;
        }

        public byte[] getOldData() {
            return oldData;
        }

        public long getPrevRollPtr() {
            return prevRollPtr;
        }

        public long getPrevTxnVersionPtr() {
            return prevTxnVersionPtr;
        }

        @Override
        public String toString() {
            return String.format("VersionRecord[ptr=%d, txn=%d, type=%s, row=%s, prevRow=%d, prevTxn=%d]",
                    versionPtr, transactionId, operationType, rowKey, prevRollPtr, prevTxnVersionPtr);
        }
    }

    public UndoLog(String dbPath) {
        // dbPath 保留参数仅用于兼容现有调用方
        this.txnLatestVersionPtr = new ConcurrentHashMap<>();
        this.versionRecords = new ConcurrentHashMap<>();
        this.nextVersionPtr = new AtomicLong(1);
        this.totalVersionRecords = new AtomicLong(0);
        this.totalRollbacks = new AtomicLong(0);
        this.committedTransactions = new ConcurrentHashMap<>();
    }

    /**
     * 追加一条历史版本记录（仅内存）
     *
     * @return 新记录对应的 roll_ptr / version_ptr
     */
    public long appendVersion(long transactionId, VersionType operationType,
                              String rowKey, byte[] oldData, long prevRowRollPtr) {
        // 获取事务内上一条历史版本（用于事务回滚链）
        long prevTxnVersionPtr = txnLatestVersionPtr.getOrDefault(transactionId, 0L);

        long newRollPtr = nextVersionPtr.getAndIncrement();
        VersionRecord record = new VersionRecord(
                newRollPtr,
                transactionId,
                operationType,
                rowKey,
                oldData,
                prevRowRollPtr,
                prevTxnVersionPtr
        );

        versionRecords.put(newRollPtr, record);

        txnLatestVersionPtr.put(transactionId, newRollPtr);

        totalVersionRecords.incrementAndGet();

        return newRollPtr;
    }

    /**
     * 兼容单行调用：默认没有上一行版本
     */
    public long appendVersion(long transactionId, VersionType operationType,
                              String rowKey, byte[] oldData) {
        return appendVersion(transactionId, operationType, rowKey, oldData, 0L);
    }

    /**
     * 获取事务当前最新版本指针（用于语句级回滚起点）
     */
    public long getLatestVersionPtr(long transactionId) {
        return txnLatestVersionPtr.getOrDefault(transactionId, 0L);
    }

    /**
     * 回滚到指定版本指针（语句级原子性）
     *
     * @param targetVersionPtr 语句开始前的版本指针；回滚后该指针之后的改动都会撤销
     */
    public void rollbackTo(Transaction transaction, long targetVersionPtr) {
        long transactionId = transaction.getTransactionId();
        long versionPtr = txnLatestVersionPtr.getOrDefault(transactionId, 0L);

        if (versionPtr == targetVersionPtr) {
            return;
        }

        while (versionPtr != 0 && versionPtr != targetVersionPtr) {
            VersionRecord record = versionRecords.get(versionPtr);
            if (record == null) {
                break;
            }

            undoOperation(record);

            long prevPtr = record.getPrevTxnVersionPtr();
            versionRecords.remove(versionPtr);
            versionPtr = prevPtr;
        }

        if (versionPtr == 0) {
            txnLatestVersionPtr.remove(transactionId);
        } else {
            txnLatestVersionPtr.put(transactionId, versionPtr);
        }
    }

    /**
     * 回滚事务（通过事务回滚链遍历）
     */
    public void rollback(Transaction transaction) {
        long transactionId = transaction.getTransactionId();
        long latestPtr = txnLatestVersionPtr.getOrDefault(transactionId, 0L);
        if (latestPtr == 0) {
            return;
        }

        System.out.println("Rolling back transaction " + transactionId);
        rollbackTo(transaction, 0L);
        purgeTransactionVersionRecords(transactionId);
        totalRollbacks.incrementAndGet();
    }

    /**
     * 读取历史版本记录（用于 MVCC 可见性判断）
     */
    public VersionRecord readVersionRecord(long rollPtr) {
        return versionRecords.get(rollPtr);
    }

    /**
     * 标记事务已提交。
     * 提交后不立即删除历史版本，是否回收由保留低水位决定。
     */
    public void markTransactionCommitted(long transactionId) {
        committedTransactions.put(transactionId, Boolean.TRUE);
        txnLatestVersionPtr.remove(transactionId);
    }

    /**
     * 清理对当前保留低水位之前、且已确认提交的历史版本。
     *
     * 规则：
     * - 仅回收 transactionId < retentionLowWatermark 的记录
     * - 且该事务必须已经 markTransactionCommitted
     * - 这是保守回收策略：宁可少删，也不误删仍可能被旧快照访问的版本
     *
     * @return 删除条数
     */
    public int purgeCommittedVersionsBefore(long retentionLowWatermark) {
        if (retentionLowWatermark <= 0) {
            return 0;
        }

        int purged = 0;
        for (Map.Entry<Long, VersionRecord> entry : new HashMap<>(versionRecords).entrySet()) {
            VersionRecord record = entry.getValue();
            if (record.getTransactionId() < retentionLowWatermark
                && committedTransactions.containsKey(record.getTransactionId())) {
                if (versionRecords.remove(entry.getKey(), record)) {
                    purged++;
                }
            }
        }
        return purged;
    }

    /**
     * 设置 Undo 应用器
     */
    public void setUndoApplier(UndoApplier undoApplier) {
        this.undoApplier = undoApplier;
    }

    /**
     * 执行单个Undo操作
     */
    private void undoOperation(VersionRecord record) {
        if (undoApplier == null) {
            System.err.println("Warning: Undo applier not set, cannot perform undo operations");
            return;
        }

        try {
            switch (record.getOperationType()) {
                case INSERT:
                    // INSERT 的回滚 = DELETE
                    System.out.println("  Undo INSERT: " + record.getRowKey());
                    undoApplier.deleteRecordByRowKey(record.getRowKey());
                    break;

                case UPDATE:
                    // UPDATE 的回滚 = 恢复旧数据
                    System.out.println("  Undo UPDATE: " + record.getRowKey());
                    byte[] oldData = record.getOldData();
                    if (oldData != null && oldData.length > 0) {
                        DataRecord oldRecord = RecordSerializer.deserialize(oldData);
                        if (oldRecord != null) {
                            undoApplier.updateRecordByRowKey(record.getRowKey(), oldRecord);
                        }
                    }
                    break;

                case DELETE:
                    // DELETE 的回滚 = INSERT（恢复删除的数据）
                    System.out.println("  Undo DELETE: " + record.getRowKey());
                    byte[] deletedData = record.getOldData();
                    if (deletedData != null && deletedData.length > 0) {
                        DataRecord restoredRecord = RecordSerializer.deserialize(deletedData);
                        if (restoredRecord != null) {
                            restoredRecord.setDeleted(false);
                            // 逻辑删除场景优先原地恢复；若不存在再插入
                            boolean restored = undoApplier.updateRecordByRowKey(record.getRowKey(), restoredRecord);
                            if (!restored) {
                                undoApplier.insertRecordByRowKey(record.getRowKey(), restoredRecord);
                            }
                        }
                    }
                    break;

                default:
                    break;
            }
        } catch (Exception e) {
            System.err.println("Failed to undo operation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format(
                "UndoLog Stats:\n" +
                "  Total Version Records: %d\n" +
                "  Total Rollbacks: %d\n" +
                "  Active Transactions: %d\n" +
                "  In-Memory Version Entries: %d",
                totalVersionRecords.get(),
                totalRollbacks.get(),
                txnLatestVersionPtr.size(),
                versionRecords.size()
        );
    }

    /**
     * 内存版无需刷盘
     */
    public void flush() {
        // no-op
    }

    public void close() throws IOException {
        // no-op
    }

    private void purgeTransactionVersionRecords(long transactionId) {
        Map<Long, VersionRecord> snapshot = new HashMap<>(versionRecords);
        for (Map.Entry<Long, VersionRecord> entry : snapshot.entrySet()) {
            if (entry.getValue().getTransactionId() == transactionId) {
                versionRecords.remove(entry.getKey());
            }
        }
        committedTransactions.remove(transactionId);
    }
}
