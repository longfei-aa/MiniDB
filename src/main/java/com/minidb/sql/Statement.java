package com.minidb.sql;

import com.minidb.common.Column;
import java.util.List;
import java.util.Map;

/**
 * SQL语句抽象基类
 */
public abstract class Statement {
    public enum StatementType {
        CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE,
        BEGIN, COMMIT, ROLLBACK,
        CREATE_INDEX  // 创建索引
    }

    public abstract StatementType getType();

    /**
     * CREATE TABLE语句
     */
    public static class CreateTableStatement extends Statement {
        private String tableName;
        private List<Column> columns;

        public CreateTableStatement(String tableName, List<Column> columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        @Override
        public StatementType getType() {
            return StatementType.CREATE_TABLE;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Column> getColumns() {
            return columns;
        }
    }

    /**
     * INSERT语句
     */
    public static class InsertStatement extends Statement {
        private String tableName;
        private List<String> columns;
        private List<Object> values;

        public InsertStatement(String tableName, List<String> columns, List<Object> values) {
            this.tableName = tableName;
            this.columns = columns;
            this.values = values;
        }

        @Override
        public StatementType getType() {
            return StatementType.INSERT;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<Object> getValues() {
            return values;
        }
    }

    /**
     * SELECT语句
     */
    public static class SelectStatement extends Statement {
        private String tableName;
        private List<String> columns;
        private WhereClause whereClause;

        public SelectStatement(String tableName, List<String> columns, WhereClause whereClause) {
            this.tableName = tableName;
            this.columns = columns;
            this.whereClause = whereClause;
        }

        @Override
        public StatementType getType() {
            return StatementType.SELECT;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getColumns() {
            return columns;
        }

        public WhereClause getWhereClause() {
            return whereClause;
        }
    }

    /**
     * WHERE子句
     */
    public static class WhereClause {
        private String column;
        private String operator;
        private Object value;

        public WhereClause(String column, String operator, Object value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public String getColumn() {
            return column;
        }

        public String getColumnName() {
            return column;
        }

        public String getOperator() {
            return operator;
        }

        public Object getValue() {
            return value;
        }

        public boolean evaluate(Map<String, Object> record) {
            Object recordValue = resolveValue(record, column);
            if (recordValue == null) {
                return false;
            }

            Integer compareResult = compareValues(recordValue, value);

            switch (operator) {
                case "=":
                    return recordValue.equals(value);
                case "<":
                    return compareResult != null && compareResult < 0;
                case ">":
                    return compareResult != null && compareResult > 0;
                case "<=":
                    return compareResult != null && compareResult <= 0;
                case ">=":
                    return compareResult != null && compareResult >= 0;
                case "!=":
                case "<>":
                    return !recordValue.equals(value);
            }

            return false;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Integer compareValues(Object left, Object right) {
            if (left == null || right == null) {
                return null;
            }
            if (!(left instanceof Comparable) || !(right instanceof Comparable)) {
                return null;
            }
            try {
                return ((Comparable) left).compareTo(right);
            } catch (ClassCastException e) {
                return null;
            }
        }

        private Object resolveValue(Map<String, Object> record, String columnName) {
            if (record.containsKey(columnName)) {
                return record.get(columnName);
            }
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(columnName)) {
                    return entry.getValue();
                }
            }
            return null;
        }
    }

    /**
     * UPDATE语句
     */
    public static class UpdateStatement extends Statement {
        private String tableName;
        private Map<String, Object> updates;
        private WhereClause whereClause;

        public UpdateStatement(String tableName, Map<String, Object> updates, WhereClause whereClause) {
            this.tableName = tableName;
            this.updates = updates;
            this.whereClause = whereClause;
        }

        @Override
        public StatementType getType() {
            return StatementType.UPDATE;
        }

        public String getTableName() {
            return tableName;
        }

        public Map<String, Object> getUpdates() {
            return updates;
        }

        public WhereClause getWhereClause() {
            return whereClause;
        }
    }

    /**
     * DELETE语句
     */
    public static class DeleteStatement extends Statement {
        private String tableName;
        private WhereClause whereClause;

        public DeleteStatement(String tableName, WhereClause whereClause) {
            this.tableName = tableName;
            this.whereClause = whereClause;
        }

        @Override
        public StatementType getType() {
            return StatementType.DELETE;
        }

        public String getTableName() {
            return tableName;
        }

        public WhereClause getWhereClause() {
            return whereClause;
        }
    }

    /**
     * BEGIN语句 - 开始事务
     */
    public static class BeginStatement extends Statement {
        @Override
        public StatementType getType() {
            return StatementType.BEGIN;
        }
    }

    /**
     * COMMIT语句 - 提交事务
     */
    public static class CommitStatement extends Statement {
        @Override
        public StatementType getType() {
            return StatementType.COMMIT;
        }
    }

    /**
     * ROLLBACK语句 - 回滚事务
     */
    public static class RollbackStatement extends Statement {
        @Override
        public StatementType getType() {
            return StatementType.ROLLBACK;
        }
    }

    /**
     * CREATE INDEX语句 - 创建索引
     * CREATE INDEX index_name ON table_name (column1, column2, ...)
     */
    public static class CreateIndexStatement extends Statement {
        private String indexName;
        private String tableName;
        private List<String> columnNames;

        public CreateIndexStatement(String indexName, String tableName, List<String> columnNames) {
            this.indexName = indexName;
            this.tableName = tableName;
            this.columnNames = columnNames;
        }

        @Override
        public StatementType getType() {
            return StatementType.CREATE_INDEX;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }
    }
}
