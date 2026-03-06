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

    // 索引数据存储：使用TreeMap实现有序存储
    // key: SecondaryIndexEntry (包含索引值和主键)
    // value: 该索引值对应的主键列表（非唯一索引可能有多个）
    private TreeMap<SecondaryIndexEntry, List<Integer>> indexData;

    // 统计信息
    private IndexStatistics statistics;
    private final ReentrantReadWriteLock indexLock;

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
        this.indexData = new TreeMap<>();
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
        this.indexData = new TreeMap<>();
        this.statistics = new IndexStatistics();
        this.indexLock = new ReentrantReadWriteLock();
    }

    /**
     * 插入索引条目
     */
    public void insert(SecondaryIndexEntry entry) {
        indexLock.writeLock().lock();
        try {
            List<Integer> pkList = indexData.get(entry);

            if (pkList == null) {
                // 新的索引值
                pkList = new ArrayList<>();
                pkList.add(entry.getPrimaryKey());
                indexData.put(entry, pkList);
                statistics.incrementKey(true);  // 新的不同键
            } else {
                // 已存在的索引值
                if (!pkList.contains(entry.getPrimaryKey())) {
                    pkList.add(entry.getPrimaryKey());
                    statistics.incrementKey(false);  // 不是新的不同键
                }
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
            List<Integer> pkList = indexData.get(entry);

            if (pkList == null) {
                return false;
            }

            boolean removed = pkList.remove(Integer.valueOf(entry.getPrimaryKey()));

            if (removed) {
                if (pkList.isEmpty()) {
                    // 这是最后一个主键，删除整个条目
                    indexData.remove(entry);
                    statistics.decrementKey(true);  // 删除了一个不同的键
                } else {
                    statistics.decrementKey(false);  // 还有其他主键
                }
            }

            return removed;
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
            List<SecondaryIndexEntry> results = new ArrayList<>();
            SecondaryIndexEntry lowerBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MAX_VALUE);

            for (Map.Entry<SecondaryIndexEntry, List<Integer>> entry :
                    indexData.subMap(lowerBound, true, upperBound, true).entrySet()) {
                SecondaryIndexEntry indexEntry = entry.getKey();
                List<Comparable<?>> indexValues = indexEntry.getIndexValues();
                if (indexValues.isEmpty() || !Objects.equals(indexValues.get(0), value)) {
                    continue;
                }
                for (Integer pk : entry.getValue()) {
                    results.add(new SecondaryIndexEntry(indexValues, pk));
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
            List<SecondaryIndexEntry> results = new ArrayList<>();
            SecondaryIndexEntry searchKey = new SecondaryIndexEntry(value, Integer.MAX_VALUE);

            for (Map.Entry<SecondaryIndexEntry, List<Integer>> entry : indexData.tailMap(searchKey, false).entrySet()) {
                for (Integer pk : entry.getValue()) {
                    results.add(new SecondaryIndexEntry(entry.getKey().getIndexValues(), pk));
                }
            }
            return results;
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
            List<SecondaryIndexEntry> results = new ArrayList<>();
            SecondaryIndexEntry lowerBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);

            for (Map.Entry<SecondaryIndexEntry, List<Integer>> entry : indexData.tailMap(lowerBound, true).entrySet()) {
                for (Integer pk : entry.getValue()) {
                    results.add(new SecondaryIndexEntry(entry.getKey().getIndexValues(), pk));
                }
            }
            return results;
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
            List<SecondaryIndexEntry> results = new ArrayList<>();
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MIN_VALUE);

            for (Map.Entry<SecondaryIndexEntry, List<Integer>> entry : indexData.headMap(upperBound, false).entrySet()) {
                for (Integer pk : entry.getValue()) {
                    results.add(new SecondaryIndexEntry(entry.getKey().getIndexValues(), pk));
                }
            }
            return results;
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
            List<SecondaryIndexEntry> results = new ArrayList<>();
            SecondaryIndexEntry upperBound = new SecondaryIndexEntry(value, Integer.MAX_VALUE);

            for (Map.Entry<SecondaryIndexEntry, List<Integer>> entry : indexData.headMap(upperBound, true).entrySet()) {
                for (Integer pk : entry.getValue()) {
                    results.add(new SecondaryIndexEntry(entry.getKey().getIndexValues(), pk));
                }
            }
            return results;
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

    @Override
    public String toString() {
        return String.format("SecondaryIndex{name='%s', table='%s', columns=%s, unique=%b, size=%d}",
            indexName, tableName, columnNames, isUnique, size());
    }
}
