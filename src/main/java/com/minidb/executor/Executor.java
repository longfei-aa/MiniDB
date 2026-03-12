package com.minidb.executor;

import com.minidb.common.*;
import com.minidb.index.BPlusTree;
import com.minidb.index.SecondaryIndex;
import com.minidb.index.SecondaryIndexEntry;
import com.minidb.optimizer.QueryOptimizer;
import com.minidb.optimizer.ExecutionPlan;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.transaction.Transaction;
import com.minidb.transaction.TransactionManager;
import com.minidb.log.redo.LogRecord;
import com.minidb.log.redo.RedoLog;
import com.minidb.log.undo.UndoLog;
import com.minidb.log.redo.RecoveryApplier;
import com.minidb.log.undo.UndoApplier;
import com.minidb.mvcc.MVCCManager;
import com.minidb.lock.LockManager;
import com.minidb.lock.LockMode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SQL执行引擎
 *
 * 面试版执行引擎：
 * - 单线程内核
 * - MVCC + 当前读加锁
 * - 启动自动恢复（先加载 schema，再重放 redo）
 */
public class Executor implements RecoveryApplier, UndoApplier {
    private final BufferPool bufferPool;
    private final Map<String, Table> tables;
    private final Map<String, BPlusTree> indexes;

    // 事务管理组件
    private final TransactionManager transactionManager;
    private final RedoLog redoLog;
    private final UndoLog undoLog;

    // MVCC 管理器
    private final MVCCManager mvccManager;

    // 查询优化器（规则优化）
    private final QueryOptimizer queryOptimizer;

    // 当前读锁管理器（简化版：行锁/间隙锁/临键锁）
    private final LockManager lockManager;

    private static final String DEFAULT_SESSION_ID = "__default_session__";

    // 多会话事务上下文：sessionId -> Session
    private final SessionManager sessionManager;

    // 逻辑删除后的延迟清理队列
    private final List<PurgeCandidate> pendingPurge;

    // 数据库标识（用于隔离 schema/redo 元数据）
    private final String databaseId;
    private final String metadataDirPath;

    // 最小化持久化元数据：DDL 文本日志
    private final String schemaFilePath;
    private final String tableRootsFilePath;
    private final Map<String, Integer> tableIds;
    private int nextTableId;

    private Session getSessionContext(String sessionId) {
        String normalized = (sessionId == null || sessionId.isEmpty()) ? DEFAULT_SESSION_ID : sessionId;
        return sessionManager.getOrCreateSession(normalized);
    }

    public Executor(BufferPool bufferPool) {
        this(bufferPool, "default");
    }

    public Executor(BufferPool bufferPool, String databaseId) {
        this.bufferPool = bufferPool;
        this.tables = new HashMap<>();
        this.indexes = new HashMap<>();
        this.databaseId = sanitizeDatabaseId(databaseId);
        this.metadataDirPath = Config.DATA_DIR + this.databaseId;

        // 初始化事务管理组件
        this.transactionManager = new TransactionManager();
        this.redoLog = new RedoLog(metadataDirPath);
        this.redoLog.setRecoveryApplier(this);

        // 初始化 Undo Log（内存版）
        this.undoLog = new UndoLog("undo");

        // 设置 Undo 应用器
        this.undoLog.setUndoApplier(this);

        // 初始化 MVCC 管理器
        this.mvccManager = new MVCCManager(transactionManager, undoLog);

        // 初始化查询优化器
        this.queryOptimizer = new QueryOptimizer();

        // 初始化锁管理器（用于当前读）
        this.lockManager = new LockManager();

        this.sessionManager = new SessionManager();
        this.pendingPurge = new ArrayList<>();
        this.schemaFilePath = metadataDirPath + "/schema.ddl";
        this.tableRootsFilePath = metadataDirPath + "/table_roots.meta";
        this.tableIds = new HashMap<>();
        this.nextTableId = 1;

        try {
            loadSchemaFromDisk();
            this.redoLog.recover();
            bootstrapTransactionIdFromData();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize executor with recovery", e);
        }
    }

    private String sanitizeDatabaseId(String input) {
        String normalized = (input == null || input.trim().isEmpty()) ? "default" : input.trim().toLowerCase();
        // 仅保留安全字符，避免路径穿越或奇异目录名
        String sanitized = normalized.replaceAll("[^a-z0-9._-]", "_");
        if (sanitized.isEmpty()) {
            return "default";
        }
        return sanitized;
    }

    private static final class PurgeCandidate {
        private final String tableName;
        private final int primaryKey;
        private final long deleteTrxId;
        private final long deleteRollPtr;

        private PurgeCandidate(String tableName, int primaryKey, long deleteTrxId, long deleteRollPtr) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.deleteTrxId = deleteTrxId;
            this.deleteRollPtr = deleteRollPtr;
        }
    }

    /**
     * 执行SQL语句
     */
    public ExecutionResult execute(Statement statement) throws Exception {
        return execute(DEFAULT_SESSION_ID, statement);
    }

    /**
     * 按会话执行 SQL 语句。每个 session 独立维护事务上下文。
     */
    public ExecutionResult execute(String sessionId, Statement statement) throws Exception {
        Session session = getSessionContext(sessionId);
        switch (statement.getType()) {
            case CREATE_TABLE:
                return executeCreateTable((Statement.CreateTableStatement) statement);
            case CREATE_INDEX:
                return executeCreateIndex((Statement.CreateIndexStatement) statement);
            case INSERT:
                return executeInsert(session, (Statement.InsertStatement) statement);
            case SELECT:
                return executeSelect(session, (Statement.SelectStatement) statement);
            case UPDATE:
                return executeUpdate(session, (Statement.UpdateStatement) statement);
            case DELETE:
                return executeDelete(session, (Statement.DeleteStatement) statement);
            case BEGIN:
                return executeBegin(session, (Statement.BeginStatement) statement);
            case COMMIT:
                return executeCommit(session, (Statement.CommitStatement) statement);
            case ROLLBACK:
                return executeRollback(session, (Statement.RollbackStatement) statement);
            default:
                throw new Exception("Unsupported statement type: " + statement.getType());
        }
    }

    /**
     * 执行CREATE TABLE
     */
    private ExecutionResult executeCreateTable(Statement.CreateTableStatement stmt) throws IOException {
        return executeCreateTable(stmt, true, null);
    }

    private ExecutionResult executeCreateTable(Statement.CreateTableStatement stmt, boolean persistSchema)
        throws IOException {
        return executeCreateTable(stmt, persistSchema, null);
    }

    private ExecutionResult executeCreateTable(Statement.CreateTableStatement stmt, boolean persistSchema,
                                              Integer existingRootPageId)
        throws IOException {
        String tableName = stmt.getTableName();

        if (tables.containsKey(tableName)) {
            return new ExecutionResult(false, "Table already exists: " + tableName);
        }

        Table table = new Table(tableName);
        for (Column column : stmt.getColumns()) {
            table.addColumn(column);
        }

        // 创建或加载B+树索引
        BPlusTree index;
        if (existingRootPageId != null && existingRootPageId > 0) {
            index = new BPlusTree(bufferPool, Config.BTREE_ORDER, existingRootPageId);
            table.setRootPageId(index.getRootPageId());
        } else {
            index = new BPlusTree(bufferPool);
            table.setRootPageId(index.getRootPageId());
        }

        tables.put(tableName, table);
        indexes.put(tableName, index);

        if (persistSchema) {
            try {
                appendSchemaDDL(buildCreateTableDDL(stmt));
                persistTableRoots();
            } catch (IOException e) {
                tables.remove(tableName);
                indexes.remove(tableName);
                throw e;
            }
        }

        return new ExecutionResult(true, "Table created: " + tableName);
    }

    /**
     * 执行CREATE INDEX
     * 创建二级索引并扫描现有数据构建索引
     */
    private ExecutionResult executeCreateIndex(Statement.CreateIndexStatement stmt) throws Exception {
        return executeCreateIndex(stmt, true);
    }

    private ExecutionResult executeCreateIndex(Statement.CreateIndexStatement stmt, boolean persistSchema)
        throws Exception {
        String indexName = stmt.getIndexName();
        String tableName = stmt.getTableName();
        List<String> columnNames = stmt.getColumnNames();

        // 1. 验证表是否存在
        Table table = tables.get(tableName);
        if (table == null) {
            return new ExecutionResult(false, "Table not found: " + tableName);
        }

        // 2. 验证索引是否已存在
        if (table.hasSecondaryIndex(indexName)) {
            return new ExecutionResult(false, "Index already exists: " + indexName);
        }

        // 3. 验证列是否存在并获取列类型
        List<Column.DataType> columnTypes = new ArrayList<>();
        for (String columnName : columnNames) {
            Column column = table.getColumn(columnName);
            if (column == null) {
                return new ExecutionResult(false, "Column not found: " + columnName);
            }
            columnTypes.add(column.getDataType());
        }

        // 4. 创建二级索引对象
        SecondaryIndex secondaryIndex;
        if (columnNames.size() == 1) {
            // 单列索引
            secondaryIndex = new SecondaryIndex(
                indexName,
                tableName,
                columnNames.get(0),
                columnTypes.get(0),
                false  // 非唯一索引
            );
        } else {
            // 联合索引
            secondaryIndex = new SecondaryIndex(
                indexName,
                tableName,
                columnNames,
                columnTypes,
                false  // 非唯一索引
            );
        }

        // 5. 扫描表数据，构建索引
        BPlusTree primaryIndex = indexes.get(tableName);
        TableIterator iterator = new TableIterator(primaryIndex);

        int indexedRows = 0;
        try {
            while (iterator.hasNext()) {
                Integer primaryKey = iterator.next();
                DataRecord record = primaryIndex.searchRecord(primaryKey);

                if (record != null && !record.isDeleted()) {
                    // 提取索引列的值
                    if (columnNames.size() == 1) {
                        // 单列索引
                        Object value = record.getValues().get(columnNames.get(0));
                        if (value != null && value instanceof Comparable) {
                            SecondaryIndexEntry entry = new SecondaryIndexEntry(
                                (Comparable<?>) value,
                                primaryKey
                            );
                            secondaryIndex.insert(entry);
                            indexedRows++;
                        }
                    } else {
                        // 联合索引
                        List<Comparable<?>> values = new ArrayList<>();
                        boolean allValuesPresent = true;

                        for (String columnName : columnNames) {
                            Object value = record.getValues().get(columnName);
                            if (value == null || !(value instanceof Comparable)) {
                                allValuesPresent = false;
                                break;
                            }
                            values.add((Comparable<?>) value);
                        }

                        if (allValuesPresent) {
                            SecondaryIndexEntry entry = new SecondaryIndexEntry(values, primaryKey);
                            secondaryIndex.insert(entry);
                            indexedRows++;
                        }
                    }
                }
            }
        } finally {
            iterator.close();
        }

        // 6. 将索引添加到表
        table.addSecondaryIndex(indexName, secondaryIndex);

        if (persistSchema) {
            try {
                appendSchemaDDL(buildCreateIndexDDL(stmt));
            } catch (IOException e) {
                table.removeSecondaryIndex(indexName);
                throw e;
            }
        }

        return new ExecutionResult(true,
            String.format("Index created: %s on %s(%s), indexed %d rows",
                indexName, tableName, String.join(", ", columnNames), indexedRows));
    }

    /**
     * 执行INSERT
     */
    private ExecutionResult executeInsert(Session session, Statement.InsertStatement stmt) throws Exception {
        String tableName = stmt.getTableName();
        Table table = tables.get(tableName);

        if (table == null) {
            return new ExecutionResult(false, "Table not found: " + tableName);
        }

        List<Object> values = stmt.getValues();
        if (values.size() != table.getColumns().size()) {
            return new ExecutionResult(false, "Column count mismatch");
        }

        // 构建完整的 DataRecord
        DataRecord record = new DataRecord();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column column = table.getColumns().get(i);
            record.setValue(column.getName(), values.get(i));
        }

        // 获取主键值
        int primaryKeyIndex = table.getPrimaryKeyIndex();
        if (primaryKeyIndex < 0) {
            return new ExecutionResult(false, "No primary key defined");
        }

        Object pkValue = values.get(primaryKeyIndex);
        if (!(pkValue instanceof Integer)) {
            return new ExecutionResult(false, "Primary key must be integer");
        }

        int primaryKey = (Integer) pkValue;

        // 主键唯一约束：插入前检查
        BPlusTree index = indexes.get(tableName);
        DataRecord existing = index.searchRecord(primaryKey);
        if (existing != null) {
            return new ExecutionResult(false, "Duplicate primary key: " + primaryKey);
        }

        // 判断是否在事务中
        boolean autoCommit = (session.getCurrentTransaction() == null);
        Transaction txn = session.getCurrentTransaction();

        if (autoCommit) {
            // 自动提交模式：创建临时事务
            txn = transactionManager.beginTransaction();
            redoLog.writeBeginLog(txn.getTransactionId());
        }
        long statementStartVersionPtr = mvccManager.getLatestVersionPtr(txn.getTransactionId());

        try {
            int tableLockId = getTableLockId(tableName);

            // 当前读：INSERT 前先对目标间隙 + 目标记录加锁
            acquireInsertLocks(txn, tableLockId, index, primaryKey);

            // MVCC: 处理插入操作（写入 Undo Log 并设置隐藏列）
            mvccManager.insert(txn, record, tableName, primaryKey);

            // 写 Redo Log（INSERT 日志）
            // 注意：这里简化实现，实际应该序列化整个 record
            redoLog.writeLog(
                LogRecord.LogType.INSERT,
                txn.getTransactionId(),
                tableLockId,
                primaryKey,
                null,  // oldData
                RedoLog.buildRecordPayload(tableName, primaryKey, record)
            );

            // 插入记录到主索引
            index.insertRecord(primaryKey, record);

            // 维护二级索引
            addRecordToSecondaryIndexes(table, record, primaryKey);

            // 自动提交模式：立即提交
            if (autoCommit) {
                redoLog.writeCommitLog(txn.getTransactionId());
                boolean flushedDirtyPages = bufferPool.flushAllPages();
                transactionManager.commit(txn);
                if (flushedDirtyPages) {
                    createCheckpoint();
                }
                tryPurgeDeletedRows();
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }

            return new ExecutionResult(true, "1 row inserted", 1);

        } catch (Exception e) {
            Exception rollbackError = null;
            try {
                rollbackStatementChanges(txn, statementStartVersionPtr);
            } catch (Exception ex) {
                rollbackError = ex;
            }

            // 失败时回滚
            if (autoCommit) {
                redoLog.writeAbortLog(txn.getTransactionId());
                transactionManager.rollback(txn);
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }
            if (rollbackError != null) {
                e.addSuppressed(rollbackError);
            }
            throw e;
        }
    }

    /**
     * 执行SELECT (集成 MVCC 快照读 + 查询优化器)
     */
    private ExecutionResult executeSelect(Session session, Statement.SelectStatement stmt) throws Exception {
        String tableName = stmt.getTableName();
        Table table = tables.get(tableName);

        if (table == null) {
            return new ExecutionResult(false, "Table not found: " + tableName);
        }

        BPlusTree primaryIndex = indexes.get(tableName);
        Statement.WhereClause whereClause = stmt.getWhereClause();
        List<String> selectedColumns = stmt.getColumns();

        // 使用查询优化器生成执行计划
        ExecutionPlan plan = queryOptimizer.optimize(stmt, table);

        // 判断是否在事务中
        Transaction txn = session.getCurrentTransaction();
        boolean needTempTransaction = false;

        // 如果不在事务中，创建临时只读事务用于快照读
        if (txn == null) {
            txn = transactionManager.beginTransaction();
            needTempTransaction = true;
        }

        List<DataRecord> results = new ArrayList<>();

        try {
            // 根据执行计划选择不同的执行路径
            switch (plan.getAccessMethod()) {
                case PRIMARY_KEY_LOOKUP:
                    // 主键等值查询
                    executePrimaryKeyLookup(whereClause, primaryIndex, txn, selectedColumns, results);
                    break;

                case SECONDARY_INDEX_LOOKUP:
                    // 二级索引查询（当前实现统一回表）
                    executeSecondaryIndexScan(plan, table, whereClause, primaryIndex, txn, selectedColumns, results);
                    break;

                case FULL_SCAN:
                default:
                    // 全表扫描
                    executeFullScan(whereClause, primaryIndex, txn, selectedColumns, results);
                    break;
            }

            return new ExecutionResult(true, results);

        } finally {
            // 如果是临时事务，立即提交（只读操作）
            if (needTempTransaction) {
                transactionManager.commit(txn);
            }
        }
    }

    /**
     * 执行主键查找
     */
    private void executePrimaryKeyLookup(Statement.WhereClause whereClause, BPlusTree primaryIndex,
                                        Transaction txn, List<String> selectedColumns,
                                        List<DataRecord> results) throws Exception {
        Object value = whereClause.getValue();
        if (value instanceof Integer) {
            DataRecord currentRecord = primaryIndex.searchRecord((Integer) value);

            if (currentRecord != null) {
                DataRecord visibleRecord = mvccManager.read(txn, currentRecord);
                if (visibleRecord != null) {
                    results.add(projectRecord(visibleRecord, selectedColumns));
                }
            }
        }
    }

    /**
     * 执行二级索引扫描
     */
    private void executeSecondaryIndexScan(ExecutionPlan plan, Table table, Statement.WhereClause whereClause,
                                          BPlusTree primaryIndex, Transaction txn, List<String> selectedColumns,
                                          List<DataRecord> results) throws Exception {
        String indexName = plan.getSelectedIndex();
        SecondaryIndex secondaryIndex = table.getSecondaryIndex(indexName);

        if (secondaryIndex == null) {
            // 回退到全表扫描
            executeFullScan(whereClause, primaryIndex, txn, selectedColumns, results);
            return;
        }

        // 使用二级索引查找
        Object value = whereClause.getValue();
        if (!(value instanceof Comparable<?>)) {
            executeFullScan(whereClause, primaryIndex, txn, selectedColumns, results);
            return;
        }

        List<SecondaryIndexEntry> entries;
        switch (whereClause.getOperator()) {
            case "=":
                entries = secondaryIndex.search((Comparable<?>) value);
                break;
            case ">":
                entries = secondaryIndex.searchGreaterThan((Comparable<?>) value);
                break;
            case ">=":
                entries = secondaryIndex.searchGreaterThanOrEqual((Comparable<?>) value);
                break;
            case "<":
                entries = secondaryIndex.searchLessThan((Comparable<?>) value);
                break;
            case "<=":
                entries = secondaryIndex.searchLessThanOrEqual((Comparable<?>) value);
                break;
            default:
                executeFullScan(whereClause, primaryIndex, txn, selectedColumns, results);
                return;
        }

        for (SecondaryIndexEntry entry : entries) {
            int primaryKey = entry.getPrimaryKey();
            // 当前实现统一回表（避免“覆盖索引”语义与实现不一致）
            DataRecord currentRecord = primaryIndex.searchRecord(primaryKey);
            if (currentRecord != null) {
                DataRecord visibleRecord = mvccManager.read(txn, currentRecord);
                if (visibleRecord != null) {
                    if (whereClause.evaluate(visibleRecord.getValues())) {
                        results.add(projectRecord(visibleRecord, selectedColumns));
                    }
                }
            }
        }
    }

    /**
     * 执行全表扫描
     */
    private void executeFullScan(Statement.WhereClause whereClause, BPlusTree primaryIndex,
                                Transaction txn, List<String> selectedColumns,
                                List<DataRecord> results) throws Exception {
        TableIterator iterator = new TableIterator(primaryIndex);
        try {
            while (iterator.hasNext()) {
                Integer key = iterator.next();
                DataRecord currentRecord = primaryIndex.searchRecord(key);

                if (currentRecord != null) {
                    DataRecord visibleRecord = mvccManager.read(txn, currentRecord);

                    if (visibleRecord != null) {
                        if (whereClause == null || whereClause.evaluate(visibleRecord.getValues())) {
                            results.add(projectRecord(visibleRecord, selectedColumns));
                        }
                    }
                }
            }
        } finally {
            iterator.close();
        }
    }

    /**
     * 执行UPDATE (集成 MVCC)
     */
    private ExecutionResult executeUpdate(Session session, Statement.UpdateStatement stmt) throws Exception {
        String tableName = stmt.getTableName();
        Table table = tables.get(tableName);

        if (table == null) {
            return new ExecutionResult(false, "Table not found: " + tableName);
        }

        BPlusTree index = indexes.get(tableName);
        Statement.WhereClause whereClause = stmt.getWhereClause();
        Map<String, Object> updates = stmt.getUpdates();

        // 判断是否在事务中
        boolean autoCommit = (session.getCurrentTransaction() == null);
        Transaction txn = session.getCurrentTransaction();

        if (autoCommit) {
            // 自动提交模式：创建临时事务
            txn = transactionManager.beginTransaction();
            redoLog.writeBeginLog(txn.getTransactionId());
        }
        long statementStartVersionPtr = mvccManager.getLatestVersionPtr(txn.getTransactionId());

        int updateCount = 0;
        int tableLockId = getTableLockId(tableName);
        String pkColumn = table.getColumns().get(table.getPrimaryKeyIndex()).getName();
        boolean updatingPrimaryKey = updates.containsKey(pkColumn);

        try {
            if (isPrimaryKeyEquality(whereClause, table)) {
                // 当前读 + 主键等值：记录锁
                Object pkValue = whereClause.getValue();
                if (pkValue instanceof Integer) {
                    Integer key = (Integer) pkValue;
                    acquireRowLockOrThrow(txn, tableLockId, key);

                    DataRecord oldRecord = index.searchRecord(key);

                    if (oldRecord != null && !oldRecord.isDeleted()) {
                        // 创建新记录（复制旧记录）
                        DataRecord newRecord = new DataRecord();
                        for (Map.Entry<String, Object> entry : oldRecord.getValues().entrySet()) {
                            newRecord.setValue(entry.getKey(), entry.getValue());
                        }

                        // 应用更新
                        for (Map.Entry<String, Object> entry : updates.entrySet()) {
                            newRecord.setValue(entry.getKey(), entry.getValue());
                        }

                        int targetPk = key;
                        if (updatingPrimaryKey) {
                            Object pkObj = updates.get(pkColumn);
                            if (!(pkObj instanceof Integer)) {
                                throw new Exception("Primary key must be integer");
                            }
                            targetPk = (Integer) pkObj;
                            if (targetPk != key) {
                                acquireRowLockOrThrow(txn, tableLockId, targetPk);
                                DataRecord duplicate = index.searchRecord(targetPk);
                                if (duplicate != null) {
                                    throw new Exception("Duplicate primary key: " + targetPk);
                                }
                            }
                        }

                        // MVCC: 处理更新操作（写入 Undo Log）
                        mvccManager.update(txn, oldRecord, newRecord, tableName, key);

                        if (targetPk != key) {
                            // 更新主键：先删除旧记录，再插入新记录
                            removeRecordFromSecondaryIndexes(table, oldRecord, key);
                            index.delete(key);
                            index.insertRecord(targetPk, newRecord);
                            addRecordToSecondaryIndexes(table, newRecord, targetPk);
                        } else {
                            // 不更新主键：原地更新 + 二级索引重建
                            removeRecordFromSecondaryIndexes(table, oldRecord, key);
                            index.updateRecord(key, newRecord);
                            addRecordToSecondaryIndexes(table, newRecord, key);
                        }

                        redoLog.writeLog(
                            LogRecord.LogType.UPDATE,
                            txn.getTransactionId(),
                            tableLockId,
                            key,
                            RedoLog.buildRecordPayload(tableName, key, oldRecord),
                            RedoLog.buildRecordPayload(tableName, targetPk, newRecord)
                        );
                        updateCount++;
                    }
                }
            } else {
                // 当前读 + 范围/全表：使用 gap lock + next-key lock（简化实现）
                acquireScanGapLockIfNeeded(txn, tableLockId, whereClause);

                List<Integer> keysToUpdate = new ArrayList<>();
                List<int[]> nextKeyRanges = new ArrayList<>();
                TableIterator iterator = new TableIterator(index);
                int prevKey = Integer.MIN_VALUE;
                try {
                    while (iterator.hasNext()) {
                        Integer key = iterator.next();
                        DataRecord record = index.searchRecord(key);

                        if (record != null) {
                            // 检查 WHERE 条件
                            if (!record.isDeleted() &&
                                (whereClause == null || whereClause.evaluate(record.getValues()))) {
                                keysToUpdate.add(key);
                                nextKeyRanges.add(new int[] {prevKey, key});
                            }
                        }
                        prevKey = key;
                    }
                } finally {
                    iterator.close();
                }

                for (int[] range : nextKeyRanges) {
                    acquireNextKeyLockOrThrow(txn, tableLockId, range[1], range[0], range[1]);
                }

                if (updatingPrimaryKey && keysToUpdate.size() > 1) {
                    throw new Exception("Batch update of primary key is not supported");
                }

                // 执行更新
                for (Integer key : keysToUpdate) {
                    DataRecord oldRecord = index.searchRecord(key);
                    if (oldRecord != null && !oldRecord.isDeleted()) {
                        // 创建新记录
                        DataRecord newRecord = new DataRecord();
                        for (Map.Entry<String, Object> entry : oldRecord.getValues().entrySet()) {
                            newRecord.setValue(entry.getKey(), entry.getValue());
                        }

                        // 应用更新
                        for (Map.Entry<String, Object> entry : updates.entrySet()) {
                            newRecord.setValue(entry.getKey(), entry.getValue());
                        }

                        int targetPk = key;
                        if (updatingPrimaryKey) {
                            Object pkObj = updates.get(pkColumn);
                            if (!(pkObj instanceof Integer)) {
                                throw new Exception("Primary key must be integer");
                            }
                            targetPk = (Integer) pkObj;
                            if (targetPk != key) {
                                acquireRowLockOrThrow(txn, tableLockId, targetPk);
                                DataRecord duplicate = index.searchRecord(targetPk);
                                if (duplicate != null) {
                                    throw new Exception("Duplicate primary key: " + targetPk);
                                }
                            }
                        }

                        // MVCC: 处理更新操作
                        mvccManager.update(txn, oldRecord, newRecord, tableName, key);

                        // 更新主索引和二级索引
                        removeRecordFromSecondaryIndexes(table, oldRecord, key);
                        if (targetPk != key) {
                            index.delete(key);
                            index.insertRecord(targetPk, newRecord);
                            addRecordToSecondaryIndexes(table, newRecord, targetPk);
                        } else {
                            index.updateRecord(key, newRecord);
                            addRecordToSecondaryIndexes(table, newRecord, key);
                        }

                        redoLog.writeLog(
                            LogRecord.LogType.UPDATE,
                            txn.getTransactionId(),
                            tableLockId,
                            key,
                            RedoLog.buildRecordPayload(tableName, key, oldRecord),
                            RedoLog.buildRecordPayload(tableName, targetPk, newRecord)
                        );
                        updateCount++;
                    }
                }
            }

            // 自动提交模式：立即提交
            if (autoCommit) {
                redoLog.writeCommitLog(txn.getTransactionId());
                boolean flushedDirtyPages = bufferPool.flushAllPages();
                transactionManager.commit(txn);
                if (flushedDirtyPages) {
                    createCheckpoint();
                }
                tryPurgeDeletedRows();
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }

            return new ExecutionResult(true, updateCount + " row(s) updated", updateCount);

        } catch (Exception e) {
            Exception rollbackError = null;
            try {
                rollbackStatementChanges(txn, statementStartVersionPtr);
            } catch (Exception ex) {
                rollbackError = ex;
            }

            // 失败时回滚
            if (autoCommit) {
                redoLog.writeAbortLog(txn.getTransactionId());
                transactionManager.rollback(txn);
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }
            if (rollbackError != null) {
                e.addSuppressed(rollbackError);
            }
            throw e;
        }
    }

    /**
     * 执行DELETE (集成 MVCC)
     */
    private ExecutionResult executeDelete(Session session, Statement.DeleteStatement stmt) throws Exception {
        String tableName = stmt.getTableName();
        Table table = tables.get(tableName);

        if (table == null) {
            return new ExecutionResult(false, "Table not found: " + tableName);
        }

        BPlusTree index = indexes.get(tableName);
        Statement.WhereClause whereClause = stmt.getWhereClause();

        // 判断是否在事务中
        boolean autoCommit = (session.getCurrentTransaction() == null);
        Transaction txn = session.getCurrentTransaction();

        if (autoCommit) {
            // 自动提交模式：创建临时事务
            txn = transactionManager.beginTransaction();
            redoLog.writeBeginLog(txn.getTransactionId());
        }
        long statementStartVersionPtr = mvccManager.getLatestVersionPtr(txn.getTransactionId());

        int deleteCount = 0;
        int tableLockId = getTableLockId(tableName);

        try {
            if (isPrimaryKeyEquality(whereClause, table)) {
                // 当前读 + 主键等值：记录锁
                Object pkValue = whereClause.getValue();
                if (pkValue instanceof Integer) {
                    Integer key = (Integer) pkValue;
                    acquireRowLockOrThrow(txn, tableLockId, key);
                    DataRecord oldRecord = index.searchRecord(key);

                    if (oldRecord != null && !oldRecord.isDeleted()) {
                        DataRecord beforeDelete = copyRecord(oldRecord);

                        // MVCC: 处理删除操作（写入 Undo Log，标记删除）
                        mvccManager.delete(txn, oldRecord, tableName, key);

                        // 逻辑删除：保留主记录，延迟purge再物理删除
                        index.updateRecord(key, oldRecord);
                        enqueuePurgeCandidate(tableName, key, oldRecord.getDbTrxId(), oldRecord.getDbRollPtr());

                        redoLog.writeLog(
                            LogRecord.LogType.DELETE,
                            txn.getTransactionId(),
                            tableLockId,
                            key,
                            RedoLog.buildRecordPayload(tableName, key, beforeDelete),
                            RedoLog.buildRowPayload(tableName, key)
                        );
                        deleteCount++;
                    }
                }
            } else {
                // 当前读 + 范围/全表：使用 gap lock + next-key lock（简化实现）
                acquireScanGapLockIfNeeded(txn, tableLockId, whereClause);

                // 全表扫描：先收集要删除的键，再删除（避免迭代时修改集合）
                List<Integer> keysToDelete = new ArrayList<>();
                List<int[]> nextKeyRanges = new ArrayList<>();

                TableIterator iterator = new TableIterator(index);
                int prevKey = Integer.MIN_VALUE;
                try {
                    while (iterator.hasNext()) {
                        Integer key = iterator.next();
                        DataRecord record = index.searchRecord(key);

                        if (record != null) {
                            // 检查 WHERE 条件
                            boolean matches = true;
                            if (whereClause != null) {
                                matches = whereClause.evaluate(record.getValues());
                            }

                            if (!record.isDeleted() && matches) {
                                keysToDelete.add(key);
                                nextKeyRanges.add(new int[] {prevKey, key});
                            }
                        }
                        prevKey = key;
                    }
                } finally {
                    iterator.close();
                }

                for (int[] range : nextKeyRanges) {
                    acquireNextKeyLockOrThrow(txn, tableLockId, range[1], range[0], range[1]);
                }

                // 执行删除
                for (Integer key : keysToDelete) {
                    DataRecord oldRecord = index.searchRecord(key);
                    if (oldRecord != null && !oldRecord.isDeleted()) {
                        DataRecord beforeDelete = copyRecord(oldRecord);

                        // MVCC: 处理删除操作
                        mvccManager.delete(txn, oldRecord, tableName, key);

                        // 逻辑删除 + 延迟purge
                        index.updateRecord(key, oldRecord);
                        enqueuePurgeCandidate(tableName, key, oldRecord.getDbTrxId(), oldRecord.getDbRollPtr());

                        redoLog.writeLog(
                            LogRecord.LogType.DELETE,
                            txn.getTransactionId(),
                            tableLockId,
                            key,
                            RedoLog.buildRecordPayload(tableName, key, beforeDelete),
                            RedoLog.buildRowPayload(tableName, key)
                        );
                        deleteCount++;
                    }
                }
            }

            // 自动提交模式：立即提交
            if (autoCommit) {
                redoLog.writeCommitLog(txn.getTransactionId());
                boolean flushedDirtyPages = bufferPool.flushAllPages();
                transactionManager.commit(txn);
                if (flushedDirtyPages) {
                    createCheckpoint();
                }
                tryPurgeDeletedRows();
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }

            return new ExecutionResult(true, deleteCount + " row(s) deleted", deleteCount);

        } catch (Exception e) {
            Exception rollbackError = null;
            try {
                rollbackStatementChanges(txn, statementStartVersionPtr);
            } catch (Exception ex) {
                rollbackError = ex;
            }

            // 失败时回滚
            if (autoCommit) {
                redoLog.writeAbortLog(txn.getTransactionId());
                transactionManager.rollback(txn);
                purgeUndoHistory();
                lockManager.releaseAllLocks(txn);
            }
            if (rollbackError != null) {
                e.addSuppressed(rollbackError);
            }
            throw e;
        }
    }

    /**
     * 执行BEGIN - 开始事务
     */
    private ExecutionResult executeBegin(Session session, Statement.BeginStatement stmt) throws Exception {
        if (session.getCurrentTransaction() != null) {
            return new ExecutionResult(false, "Transaction already active: " + session.getCurrentTransaction().getTransactionId());
        }

        // 创建新事务
        session.setCurrentTransaction(transactionManager.beginTransaction());

        // 写 BEGIN 日志
        redoLog.writeBeginLog(session.getCurrentTransaction().getTransactionId());

        return new ExecutionResult(true, "Transaction started: " + session.getCurrentTransaction().getTransactionId());
    }

    /**
     * 执行COMMIT - 提交事务 (集成 MVCC)
     */
    private ExecutionResult executeCommit(Session session, Statement.CommitStatement stmt) throws Exception {
        if (session.getCurrentTransaction() == null) {
            return new ExecutionResult(false, "No active transaction to commit");
        }

        Transaction txn = session.getCurrentTransaction();
        long txnId = txn.getTransactionId();

        try {
            // 写 COMMIT 日志并强制刷盘（保证持久性）
            redoLog.writeCommitLog(txnId);

            // 刷新所有脏页到磁盘
            boolean flushedDirtyPages = bufferPool.flushAllPages();

            // MVCC: 提交事务（清理 ReadView）
            mvccManager.commit(txn);

            // 提交事务
            transactionManager.commit(txn);
            if (flushedDirtyPages) {
                createCheckpoint();
            }

            // 延迟清理逻辑删除记录
            tryPurgeDeletedRows();
            purgeUndoHistory();

            // 释放当前事务持有的当前读锁
            lockManager.releaseAllLocks(txn);

            // 清除当前事务
            session.setCurrentTransaction(null);

            return new ExecutionResult(true, "Transaction committed: " + txnId);

        } catch (Exception e) {
            // 提交失败，回滚
            return executeRollbackInternal(session, txnId, e.getMessage());
        }
    }

    /**
     * 执行ROLLBACK - 回滚事务
     */
    private ExecutionResult executeRollback(Session session, Statement.RollbackStatement stmt) throws Exception {
        if (session.getCurrentTransaction() == null) {
            return new ExecutionResult(false, "No active transaction to rollback");
        }

        long txnId = session.getCurrentTransaction().getTransactionId();
        return executeRollbackInternal(session, txnId, "User requested rollback");
    }

    /**
     * 内部回滚方法 (集成 MVCC)
     */
    private ExecutionResult executeRollbackInternal(Session session, long txnId, String reason) throws Exception {
        Transaction txn = session.getCurrentTransaction();
        if (txn == null) {
            return new ExecutionResult(false, "No active transaction to rollback");
        }
        try {
            // 写 ABORT 日志
            redoLog.writeAbortLog(txnId);

            // MVCC: 使用 Undo Log 回滚数据修改
            mvccManager.rollback(txn);

            // 删除已回滚事务对应的purge候选
            discardPurgeCandidatesOfTxn(txnId);

            // 回滚事务
            transactionManager.rollback(txn);
            purgeUndoHistory();

            // 释放当前事务持有的当前读锁
            lockManager.releaseAllLocks(txn);

            // 清除当前事务
            session.setCurrentTransaction(null);

            return new ExecutionResult(true, "Transaction rolled back: " + txnId + " (" + reason + ")");

        } catch (Exception e) {
            session.setCurrentTransaction(null);
            throw new Exception("Rollback failed: " + e.getMessage(), e);
        }
    }

    /**
     * 判断是否为主键等值条件（用于当前读优化）
     */
    private boolean isPrimaryKeyEquality(Statement.WhereClause whereClause, Table table) {
        if (whereClause == null || table.getPrimaryKeyIndex() < 0) {
            return false;
        }
        String pkColumn = table.getColumns().get(table.getPrimaryKeyIndex()).getName();
        return "=".equals(whereClause.getOperator()) &&
            pkColumn.equalsIgnoreCase(whereClause.getColumnName());
    }

    /**
     * 将表名映射为锁管理器中的 tableId（简化版）
     */
    private int getTableLockId(String tableName) {
        String normalized = tableName == null ? "" : tableName.toLowerCase();
        return tableIds.computeIfAbsent(normalized, k -> nextTableId++);
    }

    /**
     * 语句级原子性：回滚本语句产生的改动，不影响事务其它语句
     */
    private void rollbackStatementChanges(Transaction txn, long statementStartVersionPtr) throws Exception {
        if (txn == null) {
            return;
        }
        try {
            mvccManager.rollbackTo(txn, statementStartVersionPtr);
        } catch (Exception rollbackEx) {
            throw new Exception("Statement rollback failed: " + rollbackEx.getMessage(), rollbackEx);
        }
    }

    private void enqueuePurgeCandidate(String tableName, int primaryKey, long deleteTrxId, long deleteRollPtr) {
        pendingPurge.add(new PurgeCandidate(tableName, primaryKey, deleteTrxId, deleteRollPtr));
    }

    private void discardPurgeCandidatesOfTxn(long transactionId) {
        Iterator<PurgeCandidate> iterator = pendingPurge.iterator();
        while (iterator.hasNext()) {
            PurgeCandidate candidate = iterator.next();
            if (candidate.deleteTrxId == transactionId) {
                iterator.remove();
            }
        }
    }

    /**
     * 延迟清理：仅清理对当前所有活跃事务都不可见的逻辑删除记录
     */
    private void tryPurgeDeletedRows() throws Exception {
        if (pendingPurge.isEmpty()) {
            return;
        }

        long retentionLowWatermark = mvccManager.getVersionRetentionLowWatermark();
        Iterator<PurgeCandidate> iterator = pendingPurge.iterator();
        while (iterator.hasNext()) {
            PurgeCandidate candidate = iterator.next();

            // 仍可能被更老快照看到，暂不清理
            if (candidate.deleteTrxId >= retentionLowWatermark) {
                continue;
            }

            BPlusTree index = indexes.get(candidate.tableName);
            Table table = tables.get(candidate.tableName);
            if (index == null || table == null) {
                iterator.remove();
                continue;
            }

            DataRecord currentRecord = index.searchRecord(candidate.primaryKey);
            if (currentRecord == null) {
                iterator.remove();
                continue;
            }

            // 行版本已变化或已恢复，取消本次purge候选
            if (!currentRecord.isDeleted()
                || currentRecord.getDbTrxId() != candidate.deleteTrxId
                || currentRecord.getDbRollPtr() != candidate.deleteRollPtr) {
                iterator.remove();
                continue;
            }

            removeRecordFromSecondaryIndexes(table, currentRecord, candidate.primaryKey);
            index.delete(candidate.primaryKey);
            iterator.remove();
        }
    }

    /**
     * 清理对活跃事务不可见的旧 undo 版本，控制内存增长。
     */
    private void purgeUndoHistory() {
        long retentionLowWatermark = mvccManager.getVersionRetentionLowWatermark();
        undoLog.purgeCommittedVersionsBefore(retentionLowWatermark);
    }

    private void createCheckpoint() {
        refreshTableRootPageIds();
        persistTableRoots();
        TransactionManager.ActiveTransactionSnapshot snapshot = transactionManager.captureSnapshot();
        redoLog.checkpoint(
            new HashSet<>(snapshot.getActiveTransactionIds()),
            Collections.emptySet()
        );
    }

    /**
     * 启动时扫描已落盘行版本，推进 nextTransactionId，防止重启后 ReadView 把已提交版本当“未来版本”。
     */
    private void bootstrapTransactionIdFromData() {
        long maxSeenTransactionId = 0L;
        for (Map.Entry<String, BPlusTree> entry : indexes.entrySet()) {
            BPlusTree index = entry.getValue();
            if (index == null) {
                continue;
            }

            TableIterator iterator = null;
            try {
                iterator = new TableIterator(index);
                while (iterator.hasNext()) {
                    Integer primaryKey = iterator.next();
                    DataRecord record = index.searchRecord(primaryKey);
                    if (record != null) {
                        maxSeenTransactionId = Math.max(maxSeenTransactionId, record.getDbTrxId());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to bootstrap transaction id from table: " + entry.getKey(), e);
            } finally {
                if (iterator != null) {
                    iterator.close();
                }
            }
        }

        long candidateNextTxnId = (maxSeenTransactionId >= Long.MAX_VALUE - 1)
            ? Long.MAX_VALUE
            : maxSeenTransactionId + 1;
        transactionManager.ensureNextTransactionIdAtLeast(candidateNextTxnId);
    }

    private DataRecord copyRecord(DataRecord source) {
        DataRecord copy = new DataRecord();
        for (Map.Entry<String, Object> entry : source.getValues().entrySet()) {
            copy.setValue(entry.getKey(), entry.getValue());
        }
        copy.setDbRowId(source.getDbRowId());
        copy.setDbTrxId(source.getDbTrxId());
        copy.setDbRollPtr(source.getDbRollPtr());
        copy.setDeleted(source.isDeleted());
        return copy;
    }

    /**
     * 按 SELECT 列表做投影；SELECT * 返回整行副本。
     */
    private DataRecord projectRecord(DataRecord source, List<String> selectedColumns) {
        if (source == null) {
            return null;
        }

        DataRecord projected = new DataRecord();
        projected.setDbRowId(source.getDbRowId());
        projected.setDbTrxId(source.getDbTrxId());
        projected.setDbRollPtr(source.getDbRollPtr());
        projected.setDeleted(source.isDeleted());

        if (isSelectAllColumns(selectedColumns)) {
            for (Map.Entry<String, Object> entry : source.getValues().entrySet()) {
                projected.setValue(entry.getKey(), entry.getValue());
            }
            return projected;
        }

        for (String selected : selectedColumns) {
            Object value = resolveColumnValue(source, selected);
            projected.setValue(selected, value);
        }
        return projected;
    }

    private boolean isSelectAllColumns(List<String> selectedColumns) {
        return selectedColumns == null
            || selectedColumns.isEmpty()
            || (selectedColumns.size() == 1 && "*".equals(selectedColumns.get(0)));
    }

    private Object resolveColumnValue(DataRecord record, String selectedColumn) {
        if (record.getValues().containsKey(selectedColumn)) {
            return record.getValues().get(selectedColumn);
        }
        for (Map.Entry<String, Object> entry : record.getValues().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(selectedColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void loadSchemaFromDisk() throws Exception {
        Map<String, Integer> tableRoots = loadTableRoots();
        File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String ddl = line.trim();
                if (ddl.isEmpty() || ddl.startsWith("#")) {
                    continue;
                }

                Statement statement = new Parser(ddl).parse();
                switch (statement.getType()) {
                    case CREATE_TABLE:
                        Statement.CreateTableStatement createTableStmt =
                            (Statement.CreateTableStatement) statement;
                        Integer rootPageId = tableRoots.get(normalizeTableName(createTableStmt.getTableName()));
                        executeCreateTable(createTableStmt, false, rootPageId);
                        break;
                    case CREATE_INDEX:
                        executeCreateIndex((Statement.CreateIndexStatement) statement, false);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void appendSchemaDDL(String ddl) throws IOException {
        File dir = new File(metadataDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFilePath, true))) {
            writer.write(ddl);
            writer.newLine();
        }
    }

    private String buildCreateTableDDL(Statement.CreateTableStatement stmt) {
        List<String> columnDefs = new ArrayList<>();
        for (Column column : stmt.getColumns()) {
            String typeSql;
            switch (column.getDataType()) {
                case INT:
                    typeSql = "INT";
                    break;
                case VARCHAR:
                    typeSql = "VARCHAR(" + column.getLength() + ")";
                    break;
                default:
                    typeSql = "VARCHAR(255)";
                    break;
            }

            if (column.isPrimaryKey()) {
                columnDefs.add(column.getName() + " " + typeSql + " PRIMARY KEY");
            } else {
                columnDefs.add(column.getName() + " " + typeSql);
            }
        }
        return "CREATE TABLE " + stmt.getTableName() + " (" + String.join(", ", columnDefs) + ")";
    }

    private String buildCreateIndexDDL(Statement.CreateIndexStatement stmt) {
        return "CREATE INDEX " + stmt.getIndexName()
            + " ON " + stmt.getTableName()
            + " (" + String.join(", ", stmt.getColumnNames()) + ")";
    }

    /**
     * INSERT 的当前读锁：
     * 1. 间隙锁（防止并发插入同一间隙）
     * 2. 行锁（保护目标主键）
     */
    private void acquireInsertLocks(Transaction txn, int tableLockId, BPlusTree index, int primaryKey) throws Exception {
        int[] gap = findInsertGap(index, primaryKey);
        acquireGapLockOrThrow(txn, tableLockId, gap[0], gap[1]);
        acquireRowLockOrThrow(txn, tableLockId, primaryKey);
    }

    /**
     * 查找插入键所在间隙 (prevKey, nextKey)
     * 若无边界，分别用 Integer.MIN_VALUE / Integer.MAX_VALUE 表示
     */
    private int[] findInsertGap(BPlusTree index, int targetKey) throws Exception {
        int prevKey = Integer.MIN_VALUE;
        int nextKey = Integer.MAX_VALUE;

        TableIterator iterator = new TableIterator(index);
        try {
            while (iterator.hasNext()) {
                int key = iterator.next();
                if (key < targetKey) {
                    prevKey = key;
                } else {
                    nextKey = key;
                    break;
                }
            }
        } finally {
            iterator.close();
        }
        return new int[] {prevKey, nextKey};
    }

    /**
     * 扫描型当前读的额外 gap lock（简化版）
     */
    private void acquireScanGapLockIfNeeded(Transaction txn, int tableLockId, Statement.WhereClause whereClause) throws Exception {
        if (whereClause == null) {
            acquireGapLockOrThrow(txn, tableLockId, Integer.MIN_VALUE, Integer.MAX_VALUE);
            return;
        }

        Object value = whereClause.getValue();
        if (!(value instanceof Integer)) {
            return;
        }
        int boundary = (Integer) value;

        switch (whereClause.getOperator()) {
            case "<":
            case "<=":
                acquireGapLockOrThrow(txn, tableLockId, Integer.MIN_VALUE, boundary);
                break;
            case ">":
            case ">=":
                acquireGapLockOrThrow(txn, tableLockId, boundary, Integer.MAX_VALUE);
                break;
            case "=":
            case "!=":
            case "<>":
                acquireGapLockOrThrow(txn, tableLockId, Integer.MIN_VALUE, Integer.MAX_VALUE);
                break;
            default:
                break;
        }
    }

    private void acquireRowLockOrThrow(Transaction txn, int tableLockId, int key) throws Exception {
        boolean locked = lockManager.acquireRowLock(txn, tableLockId, String.valueOf(key), LockMode.X);
        if (!locked) {
            throw new Exception("Current read lock conflict (ROW) on key=" + key);
        }
    }

    private void acquireGapLockOrThrow(Transaction txn, int tableLockId, int gapStart, int gapEnd) throws Exception {
        boolean locked = lockManager.acquireGapLock(txn, tableLockId, gapStart, gapEnd, LockMode.X);
        if (!locked) {
            throw new Exception(
                "Current read lock conflict (GAP) on (" + gapStart + ", " + gapEnd + ")"
            );
        }
    }

    private void acquireNextKeyLockOrThrow(Transaction txn, int tableLockId, int key, int gapStart, int gapEnd)
        throws Exception {
        boolean locked = lockManager.acquireNextKeyLock(
            txn,
            tableLockId,
            String.valueOf(key),
            gapStart,
            gapEnd,
            LockMode.X
        );
        if (!locked) {
            throw new Exception(
                "Current read lock conflict (NEXT_KEY) on (" + gapStart + ", " + gapEnd + "]"
            );
        }
    }

    /**
     * 为记录构造二级索引条目。
     * 若索引列缺失或不可比较，返回 null（与 CREATE INDEX / INSERT 的简化策略一致：跳过）。
     */
    private SecondaryIndexEntry buildSecondaryIndexEntry(SecondaryIndex secondaryIndex, DataRecord record, int primaryKey) {
        if (secondaryIndex == null || record == null) {
            return null;
        }

        List<String> columnNames = secondaryIndex.getColumnNames();
        if (secondaryIndex.isSingleColumn()) {
            Object value = record.getValues().get(columnNames.get(0));
            if (!(value instanceof Comparable)) {
                return null;
            }
            return new SecondaryIndexEntry((Comparable<?>) value, primaryKey);
        }

        List<Comparable<?>> values = new ArrayList<>();
        for (String columnName : columnNames) {
            Object value = record.getValues().get(columnName);
            if (!(value instanceof Comparable)) {
                return null;
            }
            values.add((Comparable<?>) value);
        }
        return new SecondaryIndexEntry(values, primaryKey);
    }

    private void addRecordToSecondaryIndexes(Table table, DataRecord record, int primaryKey) {
        if (table == null || record == null) {
            return;
        }
        for (SecondaryIndex secondaryIndex : table.getSecondaryIndexes().values()) {
            SecondaryIndexEntry entry = buildSecondaryIndexEntry(secondaryIndex, record, primaryKey);
            if (entry != null) {
                secondaryIndex.insert(entry);
            }
        }
    }

    private void removeRecordFromSecondaryIndexes(Table table, DataRecord record, int primaryKey) {
        if (table == null || record == null) {
            return;
        }
        for (SecondaryIndex secondaryIndex : table.getSecondaryIndexes().values()) {
            SecondaryIndexEntry entry = buildSecondaryIndexEntry(secondaryIndex, record, primaryKey);
            if (entry != null) {
                secondaryIndex.delete(entry);
            }
        }
    }

    /**
     * 获取当前事务（用于测试）
     */
    public Transaction getCurrentTransaction() {
        return getCurrentTransaction(DEFAULT_SESSION_ID);
    }

    public Transaction getCurrentTransaction(String sessionId) {
        return getSessionContext(sessionId).getCurrentTransaction();
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    /**
     * 获取事务管理器（用于测试）
     */
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    public Map<String, BPlusTree> getIndexes() {
        return indexes;
    }

    /**
     * 通过 rowKey 查找 DataRecord（用于 Undo 回滚）
     * rowKey 格式：row:tableName:primaryKeyValue
     */
    public DataRecord findRecordByRowKey(String rowKey) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return null;
        }

        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return null;
        }

        String tableName = parts[1];
        String pkValueStr = parts[2];

        // 获取表和索引
        Table table = tables.get(tableName);
        BPlusTree index = indexes.get(tableName);
        if (table == null || index == null) {
            return null;
        }

        // 转换主键值为整数
        try {
            Integer primaryKey = Integer.parseInt(pkValueStr);
            return index.searchRecord(primaryKey);
        } catch (NumberFormatException e) {
            return null;
        } catch (IOException e) {
            System.err.println("Failed to search record: " + e.getMessage());
            return null;
        }
    }

    /**
     * 通过 rowKey 更新 DataRecord（用于 Undo 回滚）
     */
    @Override
    public boolean updateRecordByRowKey(String rowKey, DataRecord newRecord) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return false;
        }

        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return false;
        }

        String tableName = parts[1];
        String pkValueStr = parts[2];

        // 获取表和索引
        Table table = tables.get(tableName);
        BPlusTree index = indexes.get(tableName);
        if (table == null || index == null) {
            return false;
        }

        // 转换主键值为整数
        try {
            Integer primaryKey = Integer.parseInt(pkValueStr);
            DataRecord oldRecord = index.searchRecord(primaryKey);
            if (oldRecord == null) {
                return false;
            }
            removeRecordFromSecondaryIndexes(table, oldRecord, primaryKey);
            index.updateRecord(primaryKey, newRecord);
            addRecordToSecondaryIndexes(table, newRecord, primaryKey);
            return true;
        } catch (Exception e) {
            System.err.println("Failed to update record: " + e.getMessage());
            return false;
        }
    }

    /**
     * 通过 rowKey 删除 DataRecord（用于 Undo 回滚）
     */
    @Override
    public boolean deleteRecordByRowKey(String rowKey) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return false;
        }

        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return false;
        }

        String tableName = parts[1];
        String pkValueStr = parts[2];

        // 获取表和索引
        Table table = tables.get(tableName);
        BPlusTree index = indexes.get(tableName);
        if (table == null || index == null) {
            return false;
        }

        // 转换主键值为整数
        try {
            Integer primaryKey = Integer.parseInt(pkValueStr);
            DataRecord oldRecord = index.searchRecord(primaryKey);
            if (oldRecord == null) {
                return false;
            }
            removeRecordFromSecondaryIndexes(table, oldRecord, primaryKey);
            return index.delete(primaryKey);
        } catch (Exception e) {
            System.err.println("Failed to delete record: " + e.getMessage());
            return false;
        }
    }

    /**
     * 通过 rowKey 插入 DataRecord（用于 Undo 回滚）
     */
    @Override
    public boolean insertRecordByRowKey(String rowKey, DataRecord record) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return false;
        }

        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return false;
        }

        String tableName = parts[1];
        String pkValueStr = parts[2];

        // 获取表和索引
        Table table = tables.get(tableName);
        BPlusTree index = indexes.get(tableName);
        if (table == null || index == null) {
            return false;
        }

        // 转换主键值为整数
        try {
            Integer primaryKey = Integer.parseInt(pkValueStr);
            index.insertRecord(primaryKey, record);
            addRecordToSecondaryIndexes(table, record, primaryKey);
            return true;
        } catch (Exception e) {
            System.err.println("Failed to insert record: " + e.getMessage());
            return false;
        }
    }

    /**
     * Redo 专用：重做插入（幂等）
     */
    @Override
    public boolean redoInsertByRowKey(String rowKey, DataRecord record) {
        DataRecord existing = findRecordByRowKey(rowKey);
        if (existing == null) {
            return insertRecordByRowKey(rowKey, record);
        }
        return updateRecordByRowKey(rowKey, record);
    }

    /**
     * Redo 专用：重做更新（幂等）
     */
    @Override
    public boolean redoUpdateByRowKey(String rowKey, DataRecord record) {
        DataRecord existing = findRecordByRowKey(rowKey);
        if (existing == null) {
            return insertRecordByRowKey(rowKey, record);
        }
        return updateRecordByRowKey(rowKey, record);
    }

    /**
     * Redo 专用：重做删除（幂等）
     */
    @Override
    public boolean redoDeleteByRowKey(String rowKey) {
        if (rowKey == null || !rowKey.startsWith("row:")) {
            return false;
        }

        String[] parts = rowKey.split(":", 3);
        if (parts.length < 3) {
            return false;
        }

        String tableName = parts[1];
        String pkValueStr = parts[2];
        Table table = tables.get(tableName);
        BPlusTree index = indexes.get(tableName);
        if (table == null || index == null) {
            return false;
        }

        int primaryKey;
        try {
            primaryKey = Integer.parseInt(pkValueStr);
        } catch (NumberFormatException e) {
            return false;
        }

        DataRecord existing;
        try {
            existing = index.searchRecord(primaryKey);
        } catch (Exception e) {
            return false;
        }
        if (existing == null || existing.isDeleted()) {
            return true;
        }

        try {
            existing.setDeleted(true);
            existing.setDbTrxId(0L);
            existing.setDbRollPtr(0L);
            index.updateRecord(primaryKey, existing);
            enqueuePurgeCandidate(tableName, primaryKey, existing.getDbTrxId(), existing.getDbRollPtr());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean redoHardDeleteByRowKey(String rowKey) {
        DataRecord existing = findRecordByRowKey(rowKey);
        if (existing == null) {
            return true;
        }
        return deleteRecordByRowKey(rowKey);
    }

    /**
     * 获取 MVCC 管理器（用于测试和 Undo 回滚）
     */
    public MVCCManager getMvccManager() {
        return mvccManager;
    }

    /**
     * 获取 UndoLog（用于测试）
     */
    public UndoLog getUndoLog() {
        return undoLog;
    }

    /**
     * 获取 RedoLog（用于测试和恢复）
     */
    public RedoLog getRedoLog() {
        return redoLog;
    }

    /**
     * 获取锁管理器（用于测试）
     */
    public LockManager getLockManager() {
        return lockManager;
    }

    public void close() throws IOException {
        try {
            for (Session session : sessionManager.getAllSessions()) {
                if (session.getCurrentTransaction() != null && session.getCurrentTransaction().isActive()) {
                    executeRollbackInternal(session, session.getCurrentTransaction().getTransactionId(), "Executor shutdown");
                }
            }
            if (bufferPool.flushAllPages()) {
                createCheckpoint();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close executor cleanly", e);
        } finally {
            redoLog.close();
        }
    }

    private void refreshTableRootPageIds() {
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            BPlusTree index = indexes.get(entry.getKey());
            if (index != null) {
                entry.getValue().setRootPageId(index.getRootPageId());
            }
        }
    }

    private Map<String, Integer> loadTableRoots() {
        Map<String, Integer> roots = new HashMap<>();
        File rootsFile = new File(tableRootsFilePath);
        if (!rootsFile.exists()) {
            return roots;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(rootsFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }

                String[] parts = trimmed.split("\\|", 2);
                if (parts.length != 2) {
                    continue;
                }
                try {
                    roots.put(normalizeTableName(parts[0]), Integer.parseInt(parts[1]));
                } catch (NumberFormatException ignore) {
                    // ignore broken line
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load table roots: " + e.getMessage());
        }

        return roots;
    }

    private void persistTableRoots() {
        File dir = new File(metadataDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableRootsFilePath, false))) {
            for (Map.Entry<String, Table> entry : tables.entrySet()) {
                writer.write(entry.getKey() + "|" + entry.getValue().getRootPageId());
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Failed to persist table roots: " + e.getMessage());
        }
    }

    private String normalizeTableName(String tableName) {
        return tableName == null ? "" : tableName.toLowerCase();
    }
}
