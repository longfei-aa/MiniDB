package com.minidb.common;

import com.minidb.index.SecondaryIndex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表定义与元数据容器。
 *
 * 职责：
 * - 保存表名、列定义、主键列位置、主索引根页
 * - 管理二级索引定义映射（indexName -> SecondaryIndex）
 */
public class Table {
    private String name;
    private List<Column> columns;
    private int rootPageId;  // B+树索引根节点页面ID
    private int primaryKeyIndex;  // 主键列索引

    // 二级索引定义：indexName -> SecondaryIndex
    private Map<String, SecondaryIndex> secondaryIndexes;  // indexName -> SecondaryIndex

    public Table(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.rootPageId = -1;
        this.primaryKeyIndex = -1;
        this.secondaryIndexes = new HashMap<>();
    }

    public void addColumn(Column column) {
        columns.add(column);
        if (column.isPrimaryKey()) {
            primaryKeyIndex = columns.size() - 1;
        }
    }

    public Column getColumn(String name) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public int getRootPageId() {
        return rootPageId;
    }

    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    public void setPrimaryKeyIndex(int primaryKeyIndex) {
        this.primaryKeyIndex = primaryKeyIndex;
    }

    // 二级索引管理方法
    public void addSecondaryIndex(String indexName, SecondaryIndex index) {
        secondaryIndexes.put(indexName, index);
    }

    public void removeSecondaryIndex(String indexName) {
        secondaryIndexes.remove(indexName);
    }

    public SecondaryIndex getSecondaryIndex(String indexName) {
        return secondaryIndexes.get(indexName);
    }

    public Map<String, SecondaryIndex> getSecondaryIndexes() {
        return new HashMap<>(secondaryIndexes);
    }

    public boolean hasSecondaryIndex(String indexName) {
        return secondaryIndexes.containsKey(indexName);
    }

    public String getPrimaryKeyColumn() {
        if (primaryKeyIndex >= 0 && primaryKeyIndex < columns.size()) {
            return columns.get(primaryKeyIndex).getName();
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(name).append("\n");
        sb.append("Columns:\n");
        for (Column column : columns) {
            sb.append("  ").append(column).append("\n");
        }
        return sb.toString();
    }
}
