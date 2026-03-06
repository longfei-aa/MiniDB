package com.minidb.optimizer;

import com.minidb.common.Table;
import com.minidb.sql.Statement;

/**
 * 查询优化器
 * 面试版：规则优化器（无成本模型）
 */
public class QueryOptimizer {
    public QueryOptimizer() {}

    /**
     * 为SELECT语句生成最优执行计划
     */
    public ExecutionPlan optimize(Statement.SelectStatement selectStmt, Table table) {
        IndexSelector indexSelector = new IndexSelector(table);
        return indexSelector.selectBestPlan(selectStmt);
    }
}
