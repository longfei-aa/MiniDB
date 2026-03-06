package com.minidb.common;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据记录
 *
 * 职责：
 * - 保存一行用户数据（列名 -> 值）
 * - 保存 MVCC 可见性相关隐藏列
 *
 * 关键字段：
 * - values: 用户列数据
 * - dbRowId: 行标识（无主键场景可使用）
 * - dbTrxId: 最近修改该版本的事务 ID
 * - dbRollPtr: Undo 版本链指针
 * - deleted: 逻辑删除标记
 */
public class DataRecord {
    // 用户数据
    private Map<String, Object> values;

    // InnoDB隐藏列
    private long dbRowId;        // 行ID（6字节，无主键时使用）
    private long dbTrxId;        // 事务ID（6字节）
    private long dbRollPtr;      // 回滚指针（7字节）

    // 版本信息
    private boolean deleted;     // 删除标记

    public DataRecord() {
        this.values = new HashMap<>();
        this.dbRowId = 0;
        this.dbTrxId = 0;
        this.dbRollPtr = 0;
        this.deleted = false;
    }

    public void setValue(String columnName, Object value) {
        values.put(columnName, value);
    }

    public Object getValue(String columnName) {
        return values.get(columnName);
    }

    public Integer getIntValue(String columnName) {
        Object value = values.get(columnName);
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return null;
    }

    public String getStringValue(String columnName) {
        Object value = values.get(columnName);
        if (value != null) {
            return value.toString();
        }
        return null;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    // 隐藏列的Getter和Setter
    public long getDbRowId() {
        return dbRowId;
    }

    public void setDbRowId(long dbRowId) {
        this.dbRowId = dbRowId;
    }

    public long getDbTrxId() {
        return dbTrxId;
    }

    public void setDbTrxId(long dbTrxId) {
        this.dbTrxId = dbTrxId;
    }

    public long getDbRollPtr() {
        return dbRollPtr;
    }

    public void setDbRollPtr(long dbRollPtr) {
        this.dbRollPtr = dbRollPtr;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return String.format("DataRecord[trxId=%d, rollPtr=%d, deleted=%b, values=%s]",
                dbTrxId, dbRollPtr, deleted, values.toString());
    }
}
