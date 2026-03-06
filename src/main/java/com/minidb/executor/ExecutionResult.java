package com.minidb.executor;

import com.minidb.common.DataRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL执行结果
 */
public class ExecutionResult {
    private boolean success;
    private String message;
    private List<DataRecord> records;
    private int affectedRows;

    public ExecutionResult(boolean success, String message) {
        this(success, message, 0);
    }

    public ExecutionResult(boolean success, String message, int affectedRows) {
        this.success = success;
        this.message = message;
        this.records = new ArrayList<>();
        this.affectedRows = affectedRows;
    }

    public ExecutionResult(boolean success, List<DataRecord> records) {
        this.success = success;
        this.records = records;
        this.message = records.size() + " rows returned";
        this.affectedRows = records.size();
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public List<DataRecord> getRecords() {
        return records;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    @Override
    public String toString() {
        if (!success) {
            return "ERROR: " + message;
        }

        if (records.isEmpty()) {
            return message;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SUCCESS: ").append(message).append("\n");
        for (DataRecord record : records) {
            sb.append(record).append("\n");
        }
        return sb.toString();
    }
}
