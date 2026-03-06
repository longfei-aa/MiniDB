package com.minidb.log.redo;

import com.minidb.common.DataRecord;

/**
 * Redo 恢复应用接口
 */
public interface RecoveryApplier {
    boolean redoInsertByRowKey(String rowKey, DataRecord record);

    boolean redoUpdateByRowKey(String rowKey, DataRecord record);

    boolean redoDeleteByRowKey(String rowKey);

    /**
     * Redo 专用：物理删除（用于主键变更等需要删除旧主键记录的场景）
     */
    boolean redoHardDeleteByRowKey(String rowKey);
}
