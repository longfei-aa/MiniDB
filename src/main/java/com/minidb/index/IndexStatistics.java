package com.minidb.index;

/**
 * 索引统计信息
 * 用于查询优化器的成本估算
 */
public class IndexStatistics {
    private long totalKeys;             // 索引总键数
    private long distinctKeys;          // 不同键值数量（基数）
    private double selectivity;         // 选择性 = distinctKeys / totalKeys
    private long lastUpdateTime;        // 最后更新时间

    public IndexStatistics() {
        this.totalKeys = 0;
        this.distinctKeys = 0;
        this.selectivity = 0.0;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 更新统计信息
     */
    public void updateStatistics(long totalKeys, long distinctKeys) {
        this.totalKeys = totalKeys;
        this.distinctKeys = distinctKeys;
        this.selectivity = (distinctKeys > 0) ? (double) distinctKeys / totalKeys : 0.0;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 增量更新：插入新键
     */
    public void incrementKey(boolean isNewDistinctKey) {
        this.totalKeys++;
        if (isNewDistinctKey) {
            this.distinctKeys++;
        }
        recalculateSelectivity();
    }

    /**
     * 增量更新：删除键
     */
    public void decrementKey(boolean wasLastOccurrence) {
        this.totalKeys--;
        if (wasLastOccurrence) {
            this.distinctKeys--;
        }
        recalculateSelectivity();
    }

    /**
     * 重新计算选择性
     */
    private void recalculateSelectivity() {
        if (totalKeys > 0) {
            this.selectivity = (double) distinctKeys / totalKeys;
        } else {
            this.selectivity = 0.0;
        }
        this.lastUpdateTime = System.currentTimeMillis();
    }

    // Getters
    public long getTotalKeys() {
        return totalKeys;
    }

    public long getDistinctKeys() {
        return distinctKeys;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public String toString() {
        return String.format("IndexStatistics{totalKeys=%d, distinctKeys=%d, selectivity=%.4f}",
            totalKeys, distinctKeys, selectivity);
    }
}
