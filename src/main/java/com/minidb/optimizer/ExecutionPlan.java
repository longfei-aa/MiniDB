package com.minidb.optimizer;

/**
 * 执行计划
 * 包含查询优化器选择的访问方式、索引和成本估算
 */
public class ExecutionPlan {
    private AccessMethod accessMethod;  // 访问方式
    private String selectedIndex;       // 选择的索引名
    private double estimatedCost;       // 估算成本

    /**
     * 访问方式枚举
     */
    public enum AccessMethod {
        FULL_SCAN,                    // 全表扫描
        PRIMARY_KEY_LOOKUP,           // 主键点查
        SECONDARY_INDEX_LOOKUP        // 二级索引点查 + 回表
    }

    /**
     * 构造执行计划
     */
    public ExecutionPlan(AccessMethod accessMethod, String selectedIndex, double estimatedCost) {
        this.accessMethod = accessMethod;
        this.selectedIndex = selectedIndex;
        this.estimatedCost = estimatedCost;
    }

    /**
     * 创建全表扫描计划
     */
    public static ExecutionPlan fullScan(double cost) {
        return new ExecutionPlan(AccessMethod.FULL_SCAN, null, cost);
    }

    /**
     * 创建主键查找计划
     */
    public static ExecutionPlan primaryKeyLookup() {
        return new ExecutionPlan(AccessMethod.PRIMARY_KEY_LOOKUP, "PRIMARY", 1.0);
    }

    /**
     * 判断是否需要回表
     */
    public boolean needsLookup() {
        return accessMethod == AccessMethod.SECONDARY_INDEX_LOOKUP;
    }

    // Getters and Setters
    public AccessMethod getAccessMethod() {
        return accessMethod;
    }

    public String getSelectedIndex() {
        return selectedIndex;
    }

    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public String toString() {
        return String.format("ExecutionPlan{method=%s, index=%s, cost=%.2f}",
            accessMethod, selectedIndex, estimatedCost);
    }
}
