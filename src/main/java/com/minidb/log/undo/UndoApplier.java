package com.minidb.log.undo;

import com.minidb.common.DataRecord;

/**
 * Undo 回滚应用接口
 */
public interface UndoApplier {
    boolean deleteRecordByRowKey(String rowKey);

    boolean updateRecordByRowKey(String rowKey, DataRecord newRecord);

    boolean insertRecordByRowKey(String rowKey, DataRecord record);
}
