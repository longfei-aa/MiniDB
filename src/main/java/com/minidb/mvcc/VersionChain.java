package com.minidb.mvcc;

import com.minidb.log.undo.UndoLog;
import com.minidb.common.DataRecord;

/**
 * 版本链管理器（基于内存历史版本存储）
 *
 * 不变量：
 * - 当前记录的 db_roll_ptr 总是指向“该行更旧的一个版本”
 * - 每条历史版本记录的 prevRollPtr 继续指向该行更早的版本
 * - 版本链只沿时间向过去遍历，不允许自环或回指未来版本
 */
public class VersionChain {
    private final UndoLog undoLog;

    public VersionChain(UndoLog undoLog) {
        this.undoLog = undoLog;
    }

    /**
     * 查找可见版本（通过 roll_ptr 遍历 undo 版本链）
     */
    public DataRecord findVisibleVersion(DataRecord currentRecord, ReadView readView) {
        if (currentRecord == null) {
            return null;
        }

        // 检查当前版本是否可见
        if (readView.isVisible(currentRecord.getDbTrxId())) {
            return currentRecord.isDeleted() ? null : currentRecord;
        }

        // 遍历版本链（通过roll_ptr）
        long rollPtr = currentRecord.getDbRollPtr();

        int chainLength = 0;
        while (rollPtr != 0 && chainLength < 10000) {
            chainLength++;

            // 从Undo Log读取历史版本
            UndoLog.VersionRecord versionRecord = undoLog.readVersionRecord(rollPtr);

            if (versionRecord == null) {
                break;  // 版本链结束
            }

            // 从历史版本记录重建旧版本，再基于该版本事务ID判断可见性
            DataRecord visibleRecord = reconstructFromVersionRecord(versionRecord);
            if (visibleRecord != null
                && readView.isVisible(visibleRecord.getDbTrxId())
                && !visibleRecord.isDeleted()) {
                return visibleRecord;
            }

            // 继续往前遍历
            rollPtr = versionRecord.getPrevRollPtr();
        }

        if (chainLength >= 10000) {
            System.err.println("Warning: Version chain too long (>=10000), possible bug!");
        }

        return null;  // 无可见版本
    }

    /**
     * 从历史版本记录重建 DataRecord。
     */
    private DataRecord reconstructFromVersionRecord(UndoLog.VersionRecord versionRecord) {
        if (versionRecord == null) {
            return null;
        }

        // 根据操作类型选择正确的数据
        byte[] dataToRestore = null;

        switch (versionRecord.getOperationType()) {
            case INSERT:
                // INSERT 的前镜像是“不存在”，继续沿链向前也不会得到更老版本
                return null;

            case DELETE:
                dataToRestore = versionRecord.getOldData();
                break;

            case UPDATE:
                dataToRestore = versionRecord.getOldData();
                break;

            default:
                return null;
        }

        // 反序列化旧数据
        if (dataToRestore != null && dataToRestore.length > 0) {
            try {
                DataRecord record = com.minidb.common.RecordSerializer.deserialize(dataToRestore);
                if (record != null) {
                    // 重建后的旧版本继续指向该行更早的历史版本
                    record.setDbRollPtr(versionRecord.getPrevRollPtr());
                }
                return record;
            } catch (Exception e) {
                System.err.println("Failed to reconstruct record from undo log: " + e.getMessage());
                return null;
            }
        }

        return null;
    }

    /**
     * 获取版本链长度（用于监控）
     */
    public int getChainLength(DataRecord currentRecord) {
        if (currentRecord == null) {
            return 0;
        }

        int length = 1;
        long rollPtr = currentRecord.getDbRollPtr();

        while (rollPtr != 0 && length < 10000) {
            UndoLog.VersionRecord versionRecord = undoLog.readVersionRecord(rollPtr);
            if (versionRecord == null) {
                break;
            }
            length++;
            rollPtr = versionRecord.getPrevRollPtr();
        }

        return length;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return undoLog.getStats();
    }

}
