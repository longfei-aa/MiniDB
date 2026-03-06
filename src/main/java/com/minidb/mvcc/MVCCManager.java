package com.minidb.mvcc;

import com.minidb.transaction.Transaction;
import com.minidb.transaction.IsolationLevel;
import com.minidb.transaction.TransactionManager;
import com.minidb.transaction.TransactionManager.ActiveTransactionSnapshot;
import com.minidb.common.DataRecord;
import com.minidb.log.undo.UndoLog;
import com.minidb.log.undo.UndoLog.VersionType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

/**
 * MVCC 管理器。
 *
 * 职责：
 * - 维护 ReadView 并基于版本链返回可见版本（快照读）
 * - 在 INSERT/UPDATE/DELETE 时写入 Undo 记录并维护隐藏列
 * - 提供事务提交/回滚与语句级回滚入口
 *
 * 关键字段：
 * - versionChain: 按 roll_ptr 遍历历史版本
 * - undoLog: 保存回滚数据
 * - transactionManager: 提供活跃事务信息
 * - readViews: REPEATABLE_READ 下的事务级快照缓存
 */
public class MVCCManager {
    private final VersionChain versionChain;
    private final UndoLog undoLog;
    private final TransactionManager transactionManager;

    // ReadView缓存：transactionId -> ReadView（REPEATABLE_READ使用）
    private final ConcurrentHashMap<Long, ReadView> readViews;

    public MVCCManager(TransactionManager transactionManager, UndoLog undoLog) {
        this.transactionManager = transactionManager;
        this.undoLog = undoLog;
        this.versionChain = new VersionChain(undoLog);
        this.readViews = new ConcurrentHashMap<>();
    }

    /**
     * 创建ReadView（事务开始时或每次读取时）
     */
    public ReadView createReadView(Transaction transaction) {
        ActiveTransactionSnapshot snapshot = transactionManager.captureSnapshot();

        ReadView readView = new ReadView(
                snapshot.getActiveTransactionIds(),
                snapshot.getNextTransactionId(),
                transaction.getTransactionId()
        );

        return readView;
    }

    /**
     * 获取ReadView（根据隔离级别）
     */
    public ReadView getReadView(Transaction transaction) {
        if (transaction.getIsolationLevel() == IsolationLevel.REPEATABLE_READ) {
            // REPEATABLE_READ：复用事务的ReadView
            return readViews.computeIfAbsent(
                    transaction.getTransactionId(),
                    k -> createReadView(transaction)
            );
        } else {
            // READ_COMMITTED：每次读取创建新ReadView
            return createReadView(transaction);
        }
    }

    /**
     * 读取数据（快照读）
     */
    public DataRecord read(Transaction transaction, DataRecord currentRecord) {
        if (currentRecord == null) {
            return null;
        }

        // 获取ReadView
        ReadView readView = getReadView(transaction);

        // 查找可见版本（遍历Undo版本链）
        return versionChain.findVisibleVersion(currentRecord, readView);
    }

    /**
     * 更新数据（写入Undo Log并更新roll_ptr）
     */
    public void update(Transaction transaction, DataRecord oldRecord, DataRecord newRecord,
                      String tableName, Object primaryKeyValue) {
        // 构造标准的 rowKey
        String rowKey = buildRowKey(tableName, primaryKeyValue);

        // 序列化旧记录（只需要旧数据用于回滚）
        byte[] oldData = serializeRecord(oldRecord);

        // 追加一条历史版本记录，串起该行的版本链和该事务的回滚链
        long prevRowRollPtr = oldRecord != null ? oldRecord.getDbRollPtr() : 0L;
        long newRollPtr = undoLog.appendVersion(
                transaction.getTransactionId(),
                VersionType.UPDATE,
                rowKey,
                oldData,
                prevRowRollPtr
        );

        // 更新新记录的隐藏列
        newRecord.setDbTrxId(transaction.getTransactionId());
        newRecord.setDbRollPtr(newRollPtr);  // 指向Undo版本链
    }

    /**
     * 插入数据
     */
    public void insert(Transaction transaction, DataRecord newRecord, String tableName, Object primaryKeyValue) {
        // 构造标准的 rowKey
        String rowKey = buildRowKey(tableName, primaryKeyValue);

        // INSERT 前该行不存在，因此历史版本内容为空
        long newRollPtr = undoLog.appendVersion(
                transaction.getTransactionId(),
                VersionType.INSERT,
                rowKey,
                null,
                0L
        );

        // 设置隐藏列
        newRecord.setDbTrxId(transaction.getTransactionId());
        newRecord.setDbRollPtr(newRollPtr);
        newRecord.setDeleted(false);
    }

    /**
     * 删除数据（标记删除）
     */
    public void delete(Transaction transaction, DataRecord oldRecord, String tableName, Object primaryKeyValue) {
        // 构造标准的 rowKey
        String rowKey = buildRowKey(tableName, primaryKeyValue);

        // 删除前先记录当前版本，供快照读和回滚恢复使用
        long prevRowRollPtr = oldRecord != null ? oldRecord.getDbRollPtr() : 0L;
        long newRollPtr = undoLog.appendVersion(
                transaction.getTransactionId(),
                VersionType.DELETE,
                rowKey,
                serializeRecord(oldRecord),
                prevRowRollPtr
        );

        // 标记删除
        oldRecord.setDbTrxId(transaction.getTransactionId());
        oldRecord.setDbRollPtr(newRollPtr);
        oldRecord.setDeleted(true);
    }

    /**
     * 事务提交
     */
    public void commit(Transaction transaction) {
        // 清理ReadView缓存
        readViews.remove(transaction.getTransactionId());

        // 标记该事务生成的历史版本已提交；是否可回收由低水位控制
        undoLog.markTransactionCommitted(transaction.getTransactionId());
    }

    /**
     * 事务回滚
     */
    public void rollback(Transaction transaction) {
        // 清理ReadView缓存
        readViews.remove(transaction.getTransactionId());

        // 执行回滚
        undoLog.rollback(transaction);
    }

    /**
     * 语句级回滚：回滚到语句开始时的undo指针
     */
    public void rollbackTo(Transaction transaction, long statementStartUndoPtr) {
        undoLog.rollbackTo(transaction, statementStartUndoPtr);
    }

    /**
     * 获取事务当前最新undo指针（用于语句级回滚）
     */
    public long getLatestVersionPtr(long transactionId) {
        return undoLog.getLatestVersionPtr(transactionId);
    }

    /**
     * 计算当前历史版本保留低水位。
     * 规则：
     * - 基线取当前活跃事务最小事务ID
     * - 若存在已缓存的 REPEATABLE_READ ReadView，则继续下探到最老快照的 minActiveTransactionId
     * - 结果越小越保守，越不容易误删仍可能被旧快照访问的版本
     */
    public long getVersionRetentionLowWatermark() {
        ActiveTransactionSnapshot snapshot = transactionManager.captureSnapshot();
        long lowWatermark = snapshot.getMinActiveTransactionId();
        for (ReadView readView : readViews.values()) {
            lowWatermark = Math.min(lowWatermark, readView.getMinActiveTransactionId());
        }
        return lowWatermark;
    }

    /**
     * 获取版本链管理器
     */
    public VersionChain getVersionChain() {
        return versionChain;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return versionChain.getStats();
    }

    /**
     * 根据表名和主键值构造 rowKey
     */
    public static String buildRowKey(String tableName, Object primaryKeyValue) {
        if (tableName == null || primaryKeyValue == null) {
            throw new IllegalArgumentException("Table name and primary key value cannot be null");
        }
        return "row:" + tableName + ":" + primaryKeyValue.toString();
    }

    /**
     * 从 rowKey 中提取主键值
     * rowKey 格式: row:tableName:primaryKeyValue
     */
    public static Object extractPrimaryKey(String rowKey) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return null;
        }
        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return null;
        }
        // 尝试转换为 Integer（简化实现，假设主键是整数）
        try {
            return Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            return parts[2];  // 返回字符串形式
        }
    }

    /**
     * 序列化DataRecord
     */
    private byte[] serializeRecord(DataRecord record) {
        if (record == null) {
            return new byte[0];
        }
        return com.minidb.common.RecordSerializer.serialize(record);
    }
}
