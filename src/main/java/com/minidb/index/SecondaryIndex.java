package com.minidb.index;

import com.minidb.common.Column;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 二级索引定义（支持单列索引和联合索引）
 *
 * 单列索引示例：
 *   columnNames = ["name"]
 *   columnTypes = [VARCHAR]
 *
 * 联合索引示例：
 *   columnNames = ["name", "age"]
 *   columnTypes = [VARCHAR, INT]
 */
public class SecondaryIndex {
    private String indexName;                      // 索引名称 (如 idx_users_name)
    private String tableName;                      // 所属表
    private List<String> columnNames;              // 索引列名列表
    private List<Column.DataType> columnTypes;     // 列数据类型列表
    private boolean isUnique;                      // 是否唯一索引
    private long createTime;                       // 创建时间

    // 索引数据存储：内存版 B+ 树（键为 SecondaryIndexEntry）
    private SecondaryIndexBPlusTree indexData;

    // 统计用：索引值（不含主键）出现次数
    private Map<IndexValuesKey, Integer> valueFrequencies;

    // 统计信息
    private IndexStatistics statistics;
    private final ReentrantReadWriteLock indexLock;

    private static final class IndexValuesKey {
        private final List<Comparable<?>> values;

        private IndexValuesKey(List<Comparable<?>> values) {
            this.values = new ArrayList<>(values);
        }

        private static IndexValuesKey from(SecondaryIndexEntry entry) {
            return new IndexValuesKey(entry.getIndexValues());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof IndexValuesKey)) {
                return false;
            }
            IndexValuesKey other = (IndexValuesKey) obj;
            return values.equals(other.values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }

    /**
     * 构造单列索引
     */
    public SecondaryIndex(String indexName, String tableName, String columnName,
                         Column.DataType columnType, boolean isUnique) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.columnNames.add(columnName);
        this.columnTypes = new ArrayList<>();
        this.columnTypes.add(columnType);
        this.isUnique = isUnique;
        this.createTime = System.currentTimeMillis();
        this.indexData = new SecondaryIndexBPlusTree();
        this.valueFrequencies = new HashMap<>();
        this.statistics = new IndexStatistics();
        this.indexLock = new ReentrantReadWriteLock();
    }

    /**
     * 构造联合索引
     */
    public SecondaryIndex(String indexName, String tableName, List<String> columnNames,
                         List<Column.DataType> columnTypes, boolean isUnique) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = new ArrayList<>(columnNames);
        this.columnTypes = new ArrayList<>(columnTypes);
        this.isUnique = isUnique;
        this.createTime = System.currentTimeMillis();
        this.indexData = new SecondaryIndexBPlusTree();
        this.valueFrequencies = new HashMap<>();
        this.statistics = new IndexStatistics();
        this.indexLock = new ReentrantReadWriteLock();
    }

    /**
     * 插入索引条目
     */
    public void insert(SecondaryIndexEntry entry) {
        indexLock.writeLock().lock();
        try {
            boolean inserted = indexData.insert(entry);
            if (inserted) {
                IndexValuesKey valuesKey = IndexValuesKey.from(entry);
                int oldCount = valueFrequencies.getOrDefault(valuesKey, 0);
                valueFrequencies.put(valuesKey, oldCount + 1);
                statistics.incrementKey(oldCount == 0);
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 删除索引条目
     */
    public boolean delete(SecondaryIndexEntry entry) {
        indexLock.writeLock().lock();
        try {
            boolean removed = indexData.delete(entry);
            if (!removed) {
                return false;
            }

            IndexValuesKey valuesKey = IndexValuesKey.from(entry);
            int oldCount = valueFrequencies.getOrDefault(valuesKey, 0);
            if (oldCount <= 1) {
                valueFrequencies.remove(valuesKey);
                statistics.decrementKey(true);
            } else {
                valueFrequencies.put(valuesKey, oldCount - 1);
                statistics.decrementKey(false);
            }

            return true;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 查找索引条目（等值查询）
     * 返回匹配的所有索引条目
     */
    public List<SecondaryIndexEntry> search(Comparable<?> value) {
        indexLock.readLock().lock();
        try {
            SecondaryIndexEntry lowerBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MAX_VALUE);
            List<SecondaryIndexEntry> range = indexData.rangeSearch(lowerBound, true, upperBound, true);
            List<SecondaryIndexEntry> results = new ArrayList<>();
            for (SecondaryIndexEntry entry : range) {
                List<Comparable<?>> indexValues = entry.getIndexValues();
                if (!indexValues.isEmpty() && Objects.equals(indexValues.get(0), value)) {
                    results.add(copyEntry(entry));
                }
            }
            return results;
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 范围查询（大于）
     */
    public List<SecondaryIndexEntry> searchGreaterThan(Comparable<?> value) {
        indexLock.readLock().lock();
        try {
            SecondaryIndexEntry lowerBound = new SecondaryIndexEntry(value, Integer.MAX_VALUE);
            return copyEntries(indexData.rangeSearch(lowerBound, false, null, false));
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 范围查询（大于等于）
     */
    public List<SecondaryIndexEntry> searchGreaterThanOrEqual(Comparable<?> value) {
        indexLock.readLock().lock();
        try {
            SecondaryIndexEntry lowerBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);
            return copyEntries(indexData.rangeSearch(lowerBound, true, null, false));
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 范围查询（小于）
     */
    public List<SecondaryIndexEntry> searchLessThan(Comparable<?> value) {
        indexLock.readLock().lock();
        try {
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);
            return copyEntries(indexData.rangeSearch(null, false, upperBound, false));
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 范围查询（小于等于）
     */
    public List<SecondaryIndexEntry> searchLessThanOrEqual(Comparable<?> value) {
        indexLock.readLock().lock();
        try {
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MAX_VALUE);
            return copyEntries(indexData.rangeSearch(null, false, upperBound, true));
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 判断是否为单列索引
     */
    public boolean isSingleColumn() {
        return columnNames.size() == 1;
    }

    /**
     * 判断是否为联合索引
     */
    public boolean isComposite() {
        return columnNames.size() > 1;
    }

    /**
     * 获取索引包含的所有列
     */
    public Set<String> getIndexedColumns() {
        return new HashSet<>(columnNames);
    }

    // Getters
    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    public List<Column.DataType> getColumnTypes() {
        return new ArrayList<>(columnTypes);
    }

    public boolean isUnique() {
        return isUnique;
    }

    public long getCreateTime() {
        return createTime;
    }

    public IndexStatistics getStatistics() {
        indexLock.readLock().lock();
        try {
            return statistics;
        } finally {
            indexLock.readLock().unlock();
        }
    }

    public int size() {
        indexLock.readLock().lock();
        try {
            return indexData.size();
        } finally {
            indexLock.readLock().unlock();
        }
    }

    private List<SecondaryIndexEntry> copyEntries(List<SecondaryIndexEntry> source) {
        List<SecondaryIndexEntry> result = new ArrayList<>(source.size());
        for (SecondaryIndexEntry entry : source) {
            result.add(copyEntry(entry));
        }
        return result;
    }

    private SecondaryIndexEntry copyEntry(SecondaryIndexEntry entry) {
        return new SecondaryIndexEntry(entry.getIndexValues(), entry.getPrimaryKey());
    }

    @Override
    public String toString() {
        return String.format("SecondaryIndex{name='%s', table='%s', columns=%s, unique=%b, size=%d}",
            indexName, tableName, columnNames, isUnique, size());
    }
}
