package com.minidb.log.redo;

import com.minidb.common.DataRecord;
import com.minidb.common.RecordSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redo Log - 重做日志
 * 实现WAL（Write-Ahead Logging）机制
 * 用于崩溃恢复和持久化保证
 */
public class RedoLog {
    // LSN生成器（Log Sequence Number）
    private final AtomicLong nextLsn;

    // 日志缓冲区
    private final List<LogRecord> logBuffer;

    // 日志文件路径
    private final String logFilePath;
    private final String checkpointFilePath;

    // 日志写入锁
    private final ReentrantLock writeLock;

    // 当前Checkpoint的LSN
    private volatile long checkpointLsn;

    // 已刷盘的最大LSN
    private volatile long flushedLsn;

    // 日志文件输出流
    private FileOutputStream logFileStream;
    private BufferedOutputStream logOutputStream;

    // 恢复执行目标（通常由 Executor 实现）
    private RecoveryApplier recoveryApplier;

    // 统计信息
    private final AtomicLong totalLogRecords;
    private final AtomicLong totalFlushes;

    // 日志缓冲区大小阈值（条数）
    private static final int LOG_BUFFER_THRESHOLD = 100;
    // 日志压缩阈值：日志文件超过该大小才执行重写压缩
    private static final long LOG_COMPACTION_THRESHOLD_BYTES = 4L * 1024L * 1024L;

    /**
     * 构造函数
     */
    public RedoLog(String dbPath) {
        this.nextLsn = new AtomicLong(1);
        this.logBuffer = new ArrayList<>();
        this.logFilePath = dbPath + "/redo.log";
        this.checkpointFilePath = dbPath + "/checkpoint.meta";
        this.writeLock = new ReentrantLock();
        this.checkpointLsn = 0;
        this.flushedLsn = 0;
        this.totalLogRecords = new AtomicLong(0);
        this.totalFlushes = new AtomicLong(0);

        // 创建日志文件
        try {
            openLogOutputStream(true);
            initializeNextLsnFromFile();
            this.checkpointLsn = loadCheckpointLsn();
            if (this.checkpointLsn > this.flushedLsn) {
                this.checkpointLsn = this.flushedLsn;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create redo log file: " + logFilePath, e);
        }
    }

    /**
     * 写入日志记录（INSERT/UPDATE/DELETE）
     */
    public long writeLog(LogRecord.LogType type, long transactionId, int tableId, int pageId,
                         byte[] oldData, byte[] newData) {
        writeLock.lock();
        try {
            long lsn = nextLsn.getAndIncrement();
            LogRecord record = new LogRecord(
                    lsn, type, transactionId, tableId, pageId,
                    oldData, newData, 0
            );

            // 添加到缓冲区
            logBuffer.add(record);
            totalLogRecords.incrementAndGet();

            // 如果缓冲区满，自动刷盘
            if (logBuffer.size() >= LOG_BUFFER_THRESHOLD) {
                flushLogs();
            }

            return lsn;

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 写入BEGIN日志
     */
    public long writeBeginLog(long transactionId) {
        return writeLog(LogRecord.LogType.BEGIN, transactionId, 0, 0, null, null);
    }

    /**
     * 写入COMMIT日志
     */
    public long writeCommitLog(long transactionId) {
        writeLock.lock();
        try {
            long lsn = writeLog(LogRecord.LogType.COMMIT, transactionId, 0, 0, null, null);
            // COMMIT必须立即刷盘（WAL规则）
            flushLogs();
            return lsn;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 写入ABORT日志
     */
    public long writeAbortLog(long transactionId) {
        return writeLog(LogRecord.LogType.ABORT, transactionId, 0, 0, null, null);
    }

    /**
     * 设置恢复执行目标（一般为 Executor）
     */
    public void setRecoveryApplier(RecoveryApplier recoveryApplier) {
        this.recoveryApplier = recoveryApplier;
    }

    /**
     * 刷新日志到磁盘（Force操作）
     */
    public void flushLogs() {
        writeLock.lock();
        try {
            if (logBuffer.isEmpty()) {
                return;
            }

            // 写入所有缓冲的日志
            for (LogRecord record : logBuffer) {
                writeLogToDisk(record);
            }

            // 刷新到磁盘
            logOutputStream.flush();
            forceLogFile();

            // 更新已刷盘LSN
            if (!logBuffer.isEmpty()) {
                flushedLsn = logBuffer.get(logBuffer.size() - 1).getLsn();
            }

            // 清空缓冲区
            logBuffer.clear();

            // 更新统计
            totalFlushes.incrementAndGet();

        } catch (IOException e) {
            throw new RuntimeException("Failed to flush redo log", e);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 写入单条日志到磁盘
     */
    private void writeLogToDisk(LogRecord record) throws IOException {
        // V2 文本格式：
        // V2|LSN|TYPE|TXN_ID|TABLE_ID|PAGE_ID|OLD_DATA_B64|NEW_DATA_B64
        // OLD/NEW 数据为空时写 "-"
        String logLine = formatLogLine(record);
        logOutputStream.write(logLine.getBytes(StandardCharsets.UTF_8));
    }

    private String formatLogLine(LogRecord record) {
        return String.format(
                "V2|%d|%s|%d|%d|%d|%s|%s%n",
                record.getLsn(),
                record.getType(),
                record.getTransactionId(),
                record.getTableId(),
                record.getPageId(),
                encodeBytes(record.getOldData()),
                encodeBytes(record.getNewData())
        );
    }

    /**
     * 创建Checkpoint
     * 记录当前所有脏页和活跃事务的状态
     */
    public void checkpoint(Set<Long> activeTransactionIds, Set<Integer> dirtyPageIds) {
        writeLock.lock();
        try {
            // 先刷新所有pending的日志
            flushLogs();

            // 写入checkpoint记录
            long lsn = nextLsn.getAndIncrement();
            LogRecord checkpointRecord = new LogRecord(
                    lsn,
                    LogRecord.LogType.CHECKPOINT,
                    0,
                    0,
                    0,
                    null,
                    null,
                    0
            );

            logBuffer.add(checkpointRecord);
            flushLogs();

            // 更新checkpoint LSN并持久化
            checkpointLsn = lsn;
            persistCheckpointLsn(lsn);
            if (shouldCompactLog()) {
                compactLogFromLsn(checkpointLsn);
            }

            System.out.println("Checkpoint created at LSN: " + lsn);

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 崩溃恢复（简化实现）
     * 三阶段：Analysis -> Redo -> Undo
     *
     * 说明：
     * - 先重放已提交事务的数据日志（INSERT/UPDATE/DELETE）
     * - 再对未提交事务执行逆序回滚
     */
    public void recover() {
        writeLock.lock();
        try {
            System.out.println("Starting crash recovery from redo log...");

            // 读取日志文件中的所有记录
            List<LogRecord> allRecords = readLogFile();

            if (allRecords.isEmpty()) {
                System.out.println("No log records found, recovery skipped.");
                return;
            }

            // === 阶段 1: Analysis ===
            System.out.println("Phase 1: Analysis - Scanning log from LSN " + checkpointLsn);

            Map<Long, Long> activeTransactions = new HashMap<>();
            Set<Long> committedTransactions = new HashSet<>();
            long redoLSN = checkpointLsn > 0 ? checkpointLsn : 1;

            for (LogRecord record : allRecords) {
                if (record.getLsn() < checkpointLsn) {
                    continue;
                }

                long txnId = record.getTransactionId();

                switch (record.getType()) {
                    case BEGIN:
                        activeTransactions.put(txnId, record.getLsn());
                        break;

                    case INSERT:
                    case UPDATE:
                    case DELETE:
                        activeTransactions.put(txnId, record.getLsn());
                        break;

                    case COMMIT:
                        activeTransactions.remove(txnId);
                        committedTransactions.add(txnId);
                        break;

                    case ABORT:
                        activeTransactions.remove(txnId);
                        break;

                    default:
                        break;
                }
            }

            System.out.println("Analysis complete:");
            System.out.println("  Committed transactions: " + committedTransactions.size());
            System.out.println("  Active (uncommitted) transactions: " + activeTransactions.size());

            // === 阶段 2: Redo ===
            System.out.println("Phase 2: Redo - Replaying logs from LSN " + redoLSN);

            if (recoveryApplier == null) {
                System.out.println("  Recovery applier not set. Redo apply skipped.");
                return;
            }

            int redoCount = 0;
            for (LogRecord record : allRecords) {
                if (record.getLsn() < redoLSN) {
                    continue;
                }

                // 重做数据修改操作
                if (record.getType() == LogRecord.LogType.INSERT ||
                    record.getType() == LogRecord.LogType.UPDATE ||
                    record.getType() == LogRecord.LogType.DELETE) {
                    if (committedTransactions.contains(record.getTransactionId())) {
                        if (applyRedoRecord(record)) {
                            redoCount++;
                        }
                    }
                }
            }

            System.out.println("Redo complete: " + redoCount + " operations replayed");

            // === 阶段 3: Undo ===
            Set<Long> uncommittedTxnIds = new HashSet<>(activeTransactions.keySet());
            int undoCount = 0;
            if (uncommittedTxnIds.isEmpty()) {
                System.out.println("Phase 3: Undo - no uncommitted transactions.");
            } else {
                System.out.println("Phase 3: Undo - rolling back uncommitted transactions.");
                for (int i = allRecords.size() - 1; i >= 0; i--) {
                    LogRecord record = allRecords.get(i);
                    long txnId = record.getTransactionId();
                    if (!uncommittedTxnIds.contains(txnId)) {
                        continue;
                    }

                    if (record.getType() == LogRecord.LogType.INSERT
                        || record.getType() == LogRecord.LogType.UPDATE
                        || record.getType() == LogRecord.LogType.DELETE) {
                        if (applyUndoRecord(record)) {
                            undoCount++;
                        }
                    } else if (record.getType() == LogRecord.LogType.BEGIN) {
                        uncommittedTxnIds.remove(txnId);
                    }
                }
                System.out.println("Undo complete: " + undoCount + " operations rolled back");
            }
            System.out.println("=== Recovery completed successfully ===");

        } catch (Exception e) {
            System.err.println("Recovery failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 读取日志文件中的所有记录（用于恢复）
     */
    private List<LogRecord> readLogFile() {
        List<LogRecord> records = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(logFilePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                try {
                    LogRecord record = parseLogLine(line);
                    if (record != null) {
                        records.add(record);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to parse log line: " + line);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Log file not found, no recovery needed.");
        } catch (IOException e) {
            System.err.println("Failed to read log file: " + e.getMessage());
        }

        return records;
    }

    private LogRecord parseLogLine(String line) {
        if (line.startsWith("V2|")) {
            String[] parts = line.split("\\|", 8);
            if (parts.length < 8) {
                return null;
            }

            long lsn = Long.parseLong(parts[1]);
            LogRecord.LogType type = LogRecord.LogType.valueOf(parts[2]);
            long txnId = Long.parseLong(parts[3]);
            int tableId = Integer.parseInt(parts[4]);
            int pageId = Integer.parseInt(parts[5]);
            byte[] oldData = decodeBytes(parts[6]);
            byte[] newData = decodeBytes(parts[7]);
            return new LogRecord(lsn, type, txnId, tableId, pageId, oldData, newData, 0);
        }

        // 兼容旧日志格式：
        // LSN|TYPE|TXN_ID|TABLE_ID|PAGE_ID|OLD_SIZE|NEW_SIZE
        String[] parts = line.split("\\|");
        if (parts.length < 3) {
            return null;
        }
        long lsn = Long.parseLong(parts[0]);
        LogRecord.LogType type = LogRecord.LogType.valueOf(parts[1]);
        long txnId = Long.parseLong(parts[2]);
        int tableId = parts.length > 3 ? Integer.parseInt(parts[3]) : 0;
        int pageId = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
        return new LogRecord(lsn, type, txnId, tableId, pageId, null, null, 0);
    }

    private void initializeNextLsnFromFile() {
        long maxLsn = 0;
        List<LogRecord> records = readLogFile();
        for (LogRecord record : records) {
            maxLsn = Math.max(maxLsn, record.getLsn());
        }
        if (maxLsn > 0) {
            nextLsn.set(maxLsn + 1);
            flushedLsn = maxLsn;
        }
    }

    private boolean applyRedoRecord(LogRecord record) {
        try {
            switch (record.getType()) {
                case INSERT: {
                    RedoPayload payload = decodePayload(record.getNewData());
                    if (payload == null || payload.record == null) {
                        return false;
                    }
                    return recoveryApplier.redoInsertByRowKey(payload.rowKey(), payload.record);
                }
                case UPDATE: {
                    RedoPayload oldPayload = decodePayload(record.getOldData());
                    RedoPayload newPayload = decodePayload(record.getNewData());
                    if (newPayload == null || newPayload.record == null) {
                        return false;
                    }
                    // 主键变更：先删除旧主键，再写入新主键
                    if (oldPayload != null && oldPayload.primaryKey != newPayload.primaryKey) {
                        return recoveryApplier.redoHardDeleteByRowKey(oldPayload.rowKey())
                            && recoveryApplier.redoInsertByRowKey(newPayload.rowKey(), newPayload.record);
                    }
                    return recoveryApplier.redoUpdateByRowKey(newPayload.rowKey(), newPayload.record);
                }
                case DELETE: {
                    RedoPayload payload = decodePayload(record.getNewData());
                    if (payload == null) {
                        payload = decodePayload(record.getOldData());
                    }
                    if (payload == null) {
                        return false;
                    }
                    return recoveryApplier.redoDeleteByRowKey(payload.rowKey());
                }
                default:
                    return false;
            }
        } catch (Exception e) {
            System.err.println("Failed to apply redo record LSN=" + record.getLsn() + ": " + e.getMessage());
            return false;
        }
    }

    private boolean applyUndoRecord(LogRecord record) {
        try {
            switch (record.getType()) {
                case INSERT: {
                    RedoPayload payload = decodePayload(record.getNewData());
                    if (payload == null) {
                        return false;
                    }
                    return recoveryApplier.redoDeleteByRowKey(payload.rowKey());
                }
                case UPDATE: {
                    RedoPayload oldPayload = decodePayload(record.getOldData());
                    RedoPayload newPayload = decodePayload(record.getNewData());
                    if (oldPayload == null || oldPayload.record == null) {
                        return false;
                    }
                    // 回滚主键变更：先删除新主键，再恢复旧主键
                    if (newPayload != null && oldPayload.primaryKey != newPayload.primaryKey) {
                        return recoveryApplier.redoHardDeleteByRowKey(newPayload.rowKey())
                            && recoveryApplier.redoUpdateByRowKey(oldPayload.rowKey(), oldPayload.record);
                    }
                    return recoveryApplier.redoUpdateByRowKey(oldPayload.rowKey(), oldPayload.record);
                }
                case DELETE: {
                    RedoPayload payload = decodePayload(record.getOldData());
                    if (payload == null || payload.record == null) {
                        return false;
                    }
                    return recoveryApplier.redoUpdateByRowKey(payload.rowKey(), payload.record);
                }
                default:
                    return false;
            }
        } catch (Exception e) {
            System.err.println("Failed to apply undo record LSN=" + record.getLsn() + ": " + e.getMessage());
            return false;
        }
    }

    private String encodeBytes(byte[] data) {
        if (data == null || data.length == 0) {
            return "-";
        }
        return Base64.getEncoder().encodeToString(data);
    }

    private byte[] decodeBytes(String encoded) {
        if (encoded == null || encoded.isEmpty() || "-".equals(encoded)) {
            return null;
        }
        return Base64.getDecoder().decode(encoded);
    }

    /**
     * 构造包含行键信息的 redo payload（用于 DELETE）
     */
    public static byte[] buildRowPayload(String tableName, int primaryKey) {
        String payload = "ROW|" + tableName + "|" + primaryKey;
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 构造包含完整记录的 redo payload（用于 INSERT/UPDATE）
     */
    public static byte[] buildRecordPayload(String tableName, int primaryKey, DataRecord record) {
        byte[] recordBytes = RecordSerializer.serialize(record);
        String recordBase64 = Base64.getEncoder().encodeToString(recordBytes);
        String payload = "REC|" + tableName + "|" + primaryKey + "|" + recordBase64;
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    private RedoPayload decodePayload(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        String payload = new String(data, StandardCharsets.UTF_8);
        String[] parts = payload.split("\\|", 4);
        if (parts.length < 3) {
            return null;
        }

        if ("ROW".equals(parts[0])) {
            String tableName = parts[1];
            int primaryKey = Integer.parseInt(parts[2]);
            return new RedoPayload(tableName, primaryKey, null);
        }

        if ("REC".equals(parts[0]) && parts.length == 4) {
            String tableName = parts[1];
            int primaryKey = Integer.parseInt(parts[2]);
            byte[] recordBytes = Base64.getDecoder().decode(parts[3]);
            DataRecord record = RecordSerializer.deserialize(recordBytes);
            return new RedoPayload(tableName, primaryKey, record);
        }

        return null;
    }

    private static final class RedoPayload {
        private final String tableName;
        private final int primaryKey;
        private final DataRecord record;

        private RedoPayload(String tableName, int primaryKey, DataRecord record) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.record = record;
        }

        private String rowKey() {
            return "row:" + tableName + ":" + primaryKey;
        }
    }

    private void persistCheckpointLsn(long lsn) {
        File checkpointFile = new File(checkpointFilePath);
        File parent = checkpointFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(checkpointFilePath, false), StandardCharsets.UTF_8))) {
            writer.write(Long.toString(lsn));
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Failed to persist checkpoint LSN: " + e.getMessage());
        }
    }

    private long loadCheckpointLsn() {
        File checkpointFile = new File(checkpointFilePath);
        if (!checkpointFile.exists()) {
            return 0L;
        }
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(checkpointFile), StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            if (line == null || line.trim().isEmpty()) {
                return 0L;
            }
            return Long.parseLong(line.trim());
        } catch (Exception e) {
            System.err.println("Failed to load checkpoint LSN: " + e.getMessage());
            return 0L;
        }
    }

    /**
     * 获取已刷盘的LSN
     */
    public long getFlushedLsn() {
        return flushedLsn;
    }

    /**
     * 获取Checkpoint LSN
     */
    public long getCheckpointLsn() {
        return checkpointLsn;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format(
                "RedoLog Stats:\n" +
                "  Total Log Records: %d\n" +
                "  Total Flushes: %d\n" +
                "  Next LSN: %d\n" +
                "  Flushed LSN: %d\n" +
                "  Checkpoint LSN: %d\n" +
                "  Buffer Size: %d",
                totalLogRecords.get(),
                totalFlushes.get(),
                nextLsn.get(),
                flushedLsn,
                checkpointLsn,
                logBuffer.size()
        );
    }

    /**
     * 关闭日志系统
     */
    public void close() {
        writeLock.lock();
        try {
            // 刷新所有pending日志
            flushLogs();

            // 关闭输出流
            if (logOutputStream != null) {
                logOutputStream.close();
                logOutputStream = null;
            }
            if (logFileStream != null) {
                logFileStream.close();
                logFileStream = null;
            }

        } catch (IOException e) {
            System.err.println("Error closing redo log: " + e.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    private void openLogOutputStream(boolean append) throws IOException {
        File logFile = new File(logFilePath);
        File parent = logFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        this.logFileStream = new FileOutputStream(logFile, append);
        this.logOutputStream = new BufferedOutputStream(logFileStream);
    }

    private void forceLogFile() throws IOException {
        if (logFileStream != null) {
            logFileStream.getFD().sync();
        }
    }

    private boolean shouldCompactLog() {
        File logFile = new File(logFilePath);
        return logFile.exists() && logFile.length() >= LOG_COMPACTION_THRESHOLD_BYTES;
    }

    private void compactLogFromLsn(long keepFromLsn) {
        if (keepFromLsn <= 0) {
            return;
        }

        List<LogRecord> allRecords = readLogFile();
        if (allRecords.isEmpty()) {
            return;
        }

        List<LogRecord> kept = new ArrayList<>();
        for (LogRecord record : allRecords) {
            if (record.getLsn() >= keepFromLsn) {
                kept.add(record);
            }
        }

        try {
            if (logOutputStream != null) {
                logOutputStream.close();
                logOutputStream = null;
            }
            if (logFileStream != null) {
                logFileStream.close();
                logFileStream = null;
            }

            File logFile = new File(logFilePath);
            File parent = logFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            try (FileOutputStream fos = new FileOutputStream(logFilePath, false);
                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                for (LogRecord record : kept) {
                    bos.write(formatLogLine(record).getBytes(StandardCharsets.UTF_8));
                }
                bos.flush();
                fos.getFD().sync();
            }

            openLogOutputStream(true);
            flushedLsn = kept.isEmpty() ? 0 : kept.get(kept.size() - 1).getLsn();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compact redo log", e);
        }
    }
}
