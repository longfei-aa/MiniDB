package com.minidb.test;

import com.minidb.common.Config;
import com.minidb.common.DataRecord;
import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.log.redo.LogRecord;
import com.minidb.log.redo.RedoLog;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;

import java.io.File;
import java.util.List;

/**
 * Redo 恢复测试：
 * - 已提交事务的 INSERT/UPDATE/DELETE 能被重放
 * - 未提交事务不会被重放
 */
public class RedoRecoveryTest {

    public static void main(String[] args) {
        System.out.println("========== Redo Recovery Test ==========\n");

        try {
            cleanupDataDir();

            // 1) 准备恢复目标执行器（空表）
            DiskManager diskManager = new DiskManager("redo_recovery_target.db");
            BufferPool bufferPool = new BufferPool(diskManager, 50);
            Executor executor = new Executor(bufferPool, "redo_recovery_target");

            execute(executor, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");

            // 2) 手工生成 redo 日志
            String redoPath = Config.DATA_DIR + "redo_recovery_case";
            RedoLog writer = new RedoLog(redoPath);

            long txn1 = 1L;
            writer.writeBeginLog(txn1);

            DataRecord alice = buildUser(1, "Alice", 20);
            DataRecord bob = buildUser(1, "Bob", 20);
            DataRecord eve = buildUser(2, "Eve", 22);

            writer.writeLog(
                LogRecord.LogType.INSERT,
                txn1,
                1,
                1,
                null,
                RedoLog.buildRecordPayload("users", 1, alice)
            );
            writer.writeLog(
                LogRecord.LogType.UPDATE,
                txn1,
                1,
                1,
                RedoLog.buildRecordPayload("users", 1, alice),
                RedoLog.buildRecordPayload("users", 1, bob)
            );
            writer.writeLog(
                LogRecord.LogType.INSERT,
                txn1,
                1,
                2,
                null,
                RedoLog.buildRecordPayload("users", 2, eve)
            );
            writer.writeLog(
                LogRecord.LogType.DELETE,
                txn1,
                1,
                2,
                RedoLog.buildRecordPayload("users", 2, eve),
                RedoLog.buildRowPayload("users", 2)
            );
            writer.writeCommitLog(txn1);

            // txn2 未提交（应被忽略）
            long txn2 = 2L;
            writer.writeBeginLog(txn2);
            DataRecord mallory = buildUser(3, "Mallory", 30);
            writer.writeLog(
                LogRecord.LogType.INSERT,
                txn2,
                1,
                3,
                null,
                RedoLog.buildRecordPayload("users", 3, mallory)
            );
            writer.flushLogs(); // 模拟 crash 前脏日志已落盘但事务未提交
            writer.close();

            // 3) 执行恢复
            RedoLog recovery = new RedoLog(redoPath);
            recovery.setRecoveryApplier(executor);
            recovery.recover();
            recovery.close();

            // 4) 断言恢复结果
            ExecutionResult r1 = execute(executor, "SELECT * FROM users WHERE id = 1");
            assert r1.isSuccess() : "id=1 查询应成功";
            assert r1.getRecords().size() == 1 : "id=1 应存在 1 条记录";
            Object name = r1.getRecords().get(0).getValues().get("name");
            assert "Bob".equals(name) : "id=1 应恢复为 Bob";

            ExecutionResult r2 = execute(executor, "SELECT * FROM users WHERE id = 2");
            assert r2.isSuccess() : "id=2 查询应成功";
            assert r2.getRecords().isEmpty() : "id=2 应被 DELETE 重放删除";

            ExecutionResult r3 = execute(executor, "SELECT * FROM users WHERE id = 3");
            assert r3.isSuccess() : "id=3 查询应成功";
            assert r3.getRecords().isEmpty() : "未提交事务不应被重放";

            System.out.println("✓ Recovery replayed committed INSERT/UPDATE/DELETE");
            System.out.println("✓ Recovery skipped uncommitted transaction");
            System.out.println("\n========== Redo Recovery Test Passed ==========");

        } catch (Exception e) {
            System.err.println("Redo recovery test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static DataRecord buildUser(int id, String name, int age) {
        DataRecord record = new DataRecord();
        record.setValue("id", id);
        record.setValue("name", name);
        record.setValue("age", age);
        return record;
    }

    private static ExecutionResult execute(Executor executor, String sql) throws Exception {
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        return executor.execute(stmt);
    }

    private static void cleanupDataDir() {
        File dataDir = new File(Config.DATA_DIR);
        if (dataDir.exists()) {
            deleteDirectory(dataDir);
        }
        dataDir.mkdirs();
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
}
