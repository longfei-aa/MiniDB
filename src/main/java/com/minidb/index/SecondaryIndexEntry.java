package com.minidb.index;

import java.util.ArrayList;
import java.util.List;

/**
 * 二级索引键值对（支持单列和联合索引）
 *
 * 单列索引示例：
 *   indexValues = ["Alice"]
 *   primaryKey = 1
 *
 * 联合索引示例：
 *   indexValues = ["Alice", 25]
 *   primaryKey = 1
 */
public class SecondaryIndexEntry implements Comparable<SecondaryIndexEntry> {
    private List<Comparable<?>> indexValues;  // 索引列的值列表
    private int primaryKey;                   // 对应的主键值

    /**
     * 构造单列索引条目
     */
    public SecondaryIndexEntry(Comparable<?> singleValue, int primaryKey) {
        this.indexValues = new ArrayList<>();
        this.indexValues.add(singleValue);
        this.primaryKey = primaryKey;
    }

    /**
     * 构造联合索引条目
     */
    public SecondaryIndexEntry(List<Comparable<?>> indexValues, int primaryKey) {
        this.indexValues = new ArrayList<>(indexValues);
        this.primaryKey = primaryKey;
    }

    @Override
    public int compareTo(SecondaryIndexEntry other) {
        // 按照索引列顺序依次比较
        int minSize = Math.min(this.indexValues.size(), other.indexValues.size());

        for (int i = 0; i < minSize; i++) {
            int cmp = compareValue(this.indexValues.get(i), other.indexValues.get(i));
            if (cmp != 0) {
                return cmp;  // 第i列不相等，直接返回
            }
        }

        // 所有索引列相等，按主键排序（保证唯一性）
        return Integer.compare(this.primaryKey, other.primaryKey);
    }

    /**
     * 比较两个值（支持INT, VARCHAR, BIGINT等）
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValue(Comparable<?> v1, Comparable<?> v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return -1;
        if (v2 == null) return 1;

        // 强制类型转换进行比较
        return ((Comparable) v1).compareTo(v2);
    }

    /**
     * 获取单列索引的值（便捷方法）
     */
    public Comparable<?> getSingleValue() {
        if (indexValues.isEmpty()) {
            return null;
        }
        return indexValues.get(0);
    }

    /**
     * 获取索引值列表
     */
    public List<Comparable<?>> getIndexValues() {
        return new ArrayList<>(indexValues);
    }

    /**
     * 获取主键值
     */
    public int getPrimaryKey() {
        return primaryKey;
    }

    /**
     * 判断是否为单列索引条目
     */
    public boolean isSingleColumn() {
        return indexValues.size() == 1;
    }

    @Override
    public String toString() {
        if (isSingleColumn()) {
            return String.format("IndexEntry{value=%s, pk=%d}", getSingleValue(), primaryKey);
        } else {
            return String.format("IndexEntry{values=%s, pk=%d}", indexValues, primaryKey);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SecondaryIndexEntry)) return false;

        SecondaryIndexEntry other = (SecondaryIndexEntry) obj;
        return this.primaryKey == other.primaryKey
            && this.indexValues.equals(other.indexValues);
    }

    @Override
    public int hashCode() {
        int result = indexValues.hashCode();
        result = 31 * result + primaryKey;
        return result;
    }
}
