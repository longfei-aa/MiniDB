package com.minidb.common;

/**
 * 列定义
 */
public class Column {
    private String name;
    private DataType dataType;
    private int length;  // 用于VARCHAR等类型
    private boolean nullable;
    private boolean primaryKey;

    public enum DataType {
        INT(4),
        VARCHAR(255);

        private final int defaultSize;

        DataType(int defaultSize) {
            this.defaultSize = defaultSize;
        }

        public int getDefaultSize() {
            return defaultSize;
        }
    }

    public Column(String name, DataType dataType) {
        this(name, dataType, dataType.getDefaultSize(), true, false);
    }

    public Column(String name, DataType dataType, int length, boolean nullable, boolean primaryKey) {
        this.name = name;
        this.dataType = dataType;
        this.length = length;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return String.format("%s %s(%d) %s %s",
                name, dataType, length,
                nullable ? "NULL" : "NOT NULL",
                primaryKey ? "PRIMARY KEY" : "");
    }
}
