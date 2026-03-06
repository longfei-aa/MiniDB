package com.minidb.optimizer;

import com.minidb.common.Table;
import com.minidb.index.SecondaryIndex;
import com.minidb.sql.Statement;

import java.util.*;

/**
 * 索引选择器
 * 面试版：纯规则选择
 */
public class IndexSelector {
    private final Table table;

    public IndexSelector(Table table) {
        this.table = table;
    }

    /**
     * 为SELECT语句选择最优执行计划
     */
    public ExecutionPlan selectBestPlan(Statement.SelectStatement selectStmt) {
        Statement.WhereClause whereClause = selectStmt.getWhereClause();

        // 规则1: 主键等值查询（最高优先级）
        if (whereClause != null && isPrimaryKeyEquality(whereClause)) {
            return ExecutionPlan.primaryKeyLookup();
        }

        if (whereClause == null) {
            return ExecutionPlan.fullScan(1000.0);
        }

        if (!canUseSecondaryIndexOperator(whereClause.getOperator())) {
            return ExecutionPlan.fullScan(1000.0);
        }

        // 规则2: 二级索引按固定优先级选一个最合适的
        SecondaryIndex bestIndex = null;
        int bestScore = Integer.MAX_VALUE;
        for (SecondaryIndex index : table.getSecondaryIndexes().values()) {
            if (!canUseIndex(index, whereClause)) {
                continue;
            }
            int score = scoreIndexRule(index, whereClause);
            if (score < bestScore) {
                bestScore = score;
                bestIndex = index;
            }
        }

        if (bestIndex != null) {
            return new ExecutionPlan(
                ExecutionPlan.AccessMethod.SECONDARY_INDEX_LOOKUP,
                bestIndex.getIndexName(),
                bestScore
            );
        }

        // 规则3: 无可用索引则全表扫描
        return ExecutionPlan.fullScan(1000.0);
    }

    /**
     * 判断WHERE条件是否为主键等值查询
     */
    private boolean isPrimaryKeyEquality(Statement.WhereClause whereClause) {
        if (whereClause == null) {
            return false;
        }

        String pkColumn = table.getPrimaryKeyColumn();
        if (pkColumn == null) {
            return false;
        }

        return whereClause.getColumnName().equalsIgnoreCase(pkColumn)
            && "=".equals(whereClause.getOperator());
    }

    /**
     * 判断索引是否可用（最左前缀匹配）
     */
    private boolean canUseIndex(SecondaryIndex index, Statement.WhereClause whereClause) {
        if (whereClause == null) {
            return false;
        }

        List<String> indexColumns = index.getColumnNames();
        String whereColumn = whereClause.getColumnName();

        // 单列索引：直接匹配
        if (index.isSingleColumn()) {
            return indexColumns.get(0).equalsIgnoreCase(whereColumn);
        }

        // 联合索引：检查最左前缀
        // 目前仅支持单WHERE条件，检查是否匹配联合索引的第一列
        return indexColumns.get(0).equalsIgnoreCase(whereColumn);
    }

    private int scoreIndexRule(SecondaryIndex index, Statement.WhereClause whereClause) {
        int score = 100;
        if ("=".equals(whereClause.getOperator())) {
            score -= 50;
        } else {
            score -= 35;
        }

        // 单列索引在单条件场景更直接
        if (index.isSingleColumn()) {
            score -= 5;
        }
        return score;
    }

    private boolean canUseSecondaryIndexOperator(String operator) {
        return "=".equals(operator)
            || ">".equals(operator)
            || ">=".equals(operator)
            || "<".equals(operator)
            || "<=".equals(operator);
    }
}
