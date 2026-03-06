package com.minidb.test;

import com.minidb.common.Config;
import com.minidb.common.DataRecord;
import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.index.BPlusTree;
import com.minidb.log.undo.UndoLog;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;

import java.io.File;
import java.util.List;

/**
 * MVCC和事务功能测试
 *
 * 测试范围：
 * 1. BEGIN/COMMIT/ROLLBACK - 事务控制
 * 2. 快照读 - 事务隔离
 * 3. 并发更新
 * 4. 回滚功能
 * 5. MVCC版本可见性
 */
public class MVCCTransactionTest {

    public static void main(String[] args) {
        System.out.println("========== MiniDB MVCC与事务测试 ==========\n");

        try {
            cleanupDataDir();

            DiskManager diskManager = new DiskManager("mvcc_test.db");
            BufferPool bufferPool = new BufferPool(diskManager, 100);
            Executor executor = new Executor(bufferPool, "mvcc_test");

            // 准备测试数据
            setupTestData(executor);

            // 测试1: 基本事务提交
            testBasicTransaction(executor);

            // 测试2: 事务回滚
            testTransactionRollback(executor);

            // 测试2.5: MVCC 版本链按行连接
            testRowVersionChainLinking(executor);

            // 测试3: 快照读隔离
            testSnapshotIsolation(executor);

            // 测试4: 事务中的UPDATE和DELETE
            testTransactionModifications(executor);

            // 测试5: 自动提交模式
            testAutoCommitMode(executor);

            System.out.println("\n========== ✅ 所有MVCC和事务测试通过！ ==========");
            System.out.println("通过测试项：");
            System.out.println("  ✓ 事务BEGIN/COMMIT/ROLLBACK");
            System.out.println("  ✓ 快照读和事务隔离");
            System.out.println("  ✓ MVCC版本管理");
            System.out.println("  ✓ 回滚功能");
            System.out.println("  ✓ 自动提交模式");

        } catch (Exception e) {
            System.err.println("\n❌ 测试失败：" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void setupTestData(Executor executor) throws Exception {
        System.out.println("【准备】创建测试表并插入初始数据");

        // 创建账户表
        String sql = "CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(50), balance INT)";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        executor.execute(stmt);

        // 插入初始数据（自动提交）
        String[] inserts = {
            "INSERT INTO accounts VALUES (1, 'Alice', 1000)",
            "INSERT INTO accounts VALUES (2, 'Bob', 2000)",
            "INSERT INTO accounts VALUES (3, 'Charlie', 1500)"
        };

        for (String insertSql : inserts) {
            parser = new Parser(insertSql);
            stmt = parser.parse();
            executor.execute(stmt);
        }

        System.out.println("  ✓ 初始数据准备完成\n");
    }

    private static void testBasicTransaction(Executor executor) throws Exception {
        System.out.println("【测试1】基本事务提交");

        // 开始事务
        Parser parser = new Parser("BEGIN");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        assert result.isSuccess() : "BEGIN应该成功";
        System.out.println("  ✓ 事务已开始");

        // 在事务中插入数据
        parser = new Parser("INSERT INTO accounts VALUES (4, 'David', 3000)");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "事务中INSERT应该成功";
        System.out.println("  ✓ 事务中插入数据");

        // 提交事务
        parser = new Parser("COMMIT");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "COMMIT应该成功";
        System.out.println("  ✓ 事务已提交");

        // 验证数据已持久化
        parser = new Parser("SELECT * FROM accounts WHERE id = 4");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.getRecords().size() == 1 : "提交后应该能查到数据";
        System.out.println("  ✓ 验证：提交的数据已持久化");
    }

    private static void testTransactionRollback(Executor executor) throws Exception {
        System.out.println("\n【测试2】事务回滚");

        // 查询回滚前的记录数
        Parser parser = new Parser("SELECT * FROM accounts");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        int beforeCount = result.getRecords().size();

        // 开始事务
        parser = new Parser("BEGIN");
        stmt = parser.parse();
        executor.execute(stmt);
        System.out.println("  ✓ 事务已开始");

        // 在事务中插入数据
        parser = new Parser("INSERT INTO accounts VALUES (5, 'Eve', 4000)");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "事务中INSERT应该成功";
        System.out.println("  ✓ 事务中插入数据 (id=5)");

        // 回滚事务
        parser = new Parser("ROLLBACK");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "ROLLBACK应该成功";
        System.out.println("  ✓ 事务已回滚");

        // 验证数据未持久化
        parser = new Parser("SELECT * FROM accounts");
        stmt = parser.parse();
        result = executor.execute(stmt);
        int afterCount = result.getRecords().size();

        assert afterCount == beforeCount : "回滚后记录数应该不变";
        System.out.println("  ✓ 验证：回滚后数据未持久化（记录数: " + afterCount + "）");

        // 验证id=5的记录不存在
        parser = new Parser("SELECT * FROM accounts WHERE id = 5");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.getRecords().size() == 0 : "回滚后不应该有id=5的记录";
        System.out.println("  ✓ 验证：id=5的记录已回滚");
    }

    private static void testRowVersionChainLinking(Executor executor) throws Exception {
        System.out.println("\n【测试2.5】MVCC版本链按行连接");

        // 开启事务，交错更新两行
        Parser parser = new Parser("BEGIN");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        assert result.isSuccess() : "BEGIN应该成功";

        parser = new Parser("UPDATE accounts SET balance = 1100 WHERE id = 1");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "第一次更新id=1应成功";

        BPlusTree index = executor.getIndexes().get("accounts");
        DataRecord row1AfterFirstUpdate = index.searchRecord(1);
        assert row1AfterFirstUpdate != null : "id=1 应存在";
        long row1PtrV1 = row1AfterFirstUpdate.getDbRollPtr();

        parser = new Parser("UPDATE accounts SET balance = 2100 WHERE id = 2");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "更新id=2应成功";

        parser = new Parser("UPDATE accounts SET balance = 1200 WHERE id = 1");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "第二次更新id=1应成功";

        DataRecord row1AfterSecondUpdate = index.searchRecord(1);
        assert row1AfterSecondUpdate != null : "id=1 应存在";
        long row1PtrV2 = row1AfterSecondUpdate.getDbRollPtr();
        assert row1PtrV2 != row1PtrV1 : "第二次更新后 roll_ptr 应变化";

        UndoLog undoLog = executor.getUndoLog();
        UndoLog.VersionRecord latestRow1Undo = undoLog.readVersionRecord(row1PtrV2);
        assert latestRow1Undo != null : "应读取到id=1最新历史版本记录";
        assert latestRow1Undo.getPrevRollPtr() == row1PtrV1
            : "行版本链错误：id=1 第二次更新应链接到id=1第一次更新版本";

        // 回滚并验证两行恢复
        parser = new Parser("ROLLBACK");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "ROLLBACK应该成功";

        parser = new Parser("SELECT * FROM accounts WHERE id = 1");
        stmt = parser.parse();
        result = executor.execute(stmt);
        Object balance1 = result.getRecords().get(0).getValues().get("balance");
        assert balance1.equals(1000) : "回滚后id=1应恢复为1000";

        parser = new Parser("SELECT * FROM accounts WHERE id = 2");
        stmt = parser.parse();
        result = executor.execute(stmt);
        Object balance2 = result.getRecords().get(0).getValues().get("balance");
        assert balance2.equals(2000) : "回滚后id=2应恢复为2000";

        System.out.println("  ✓ 同一行多次更新的undo链按行连接");
        System.out.println("  ✓ 交错更新后事务回滚链正常");
    }

    private static void testSnapshotIsolation(Executor executor) throws Exception {
        System.out.println("\n【测试3】快照读隔离");

        // 注：由于是单线程测试，这里主要测试快照读机制是否正常工作
        // 在事务外查询数据（快照读）
        Parser parser = new Parser("SELECT * FROM accounts WHERE id = 1");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该查到Alice的记录";
        Object balance = records.get(0).getValues().get("balance");
        System.out.println("  ✓ 快照读: Alice的balance = " + balance);

        // 开始事务并更新
        parser = new Parser("BEGIN");
        stmt = parser.parse();
        executor.execute(stmt);

        parser = new Parser("UPDATE accounts SET balance = 5000 WHERE id = 1");
        stmt = parser.parse();
        executor.execute(stmt);
        System.out.println("  ✓ 事务中更新Alice的balance为5000");

        // 提交事务
        parser = new Parser("COMMIT");
        stmt = parser.parse();
        executor.execute(stmt);

        // 验证更新后的值
        parser = new Parser("SELECT * FROM accounts WHERE id = 1");
        stmt = parser.parse();
        result = executor.execute(stmt);
        records = result.getRecords();
        Object newBalance = records.get(0).getValues().get("balance");

        assert newBalance.equals(5000) : "提交后balance应该是5000";
        System.out.println("  ✓ 提交后查询: Alice的balance = " + newBalance);
    }

    private static void testTransactionModifications(Executor executor) throws Exception {
        System.out.println("\n【测试4】事务中的UPDATE和DELETE");

        // 开始事务
        Parser parser = new Parser("BEGIN");
        Statement stmt = parser.parse();
        executor.execute(stmt);
        System.out.println("  ✓ 事务已开始");

        // 更新操作
        parser = new Parser("UPDATE accounts SET balance = 2500 WHERE id = 2");
        stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        assert result.isSuccess() : "事务中UPDATE应该成功";
        System.out.println("  ✓ 更新Bob的balance为2500");

        // 删除操作
        parser = new Parser("DELETE FROM accounts WHERE id = 3");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "事务中DELETE应该成功";
        System.out.println("  ✓ 删除Charlie的记录");

        // 提交事务
        parser = new Parser("COMMIT");
        stmt = parser.parse();
        executor.execute(stmt);
        System.out.println("  ✓ 事务已提交");

        // 验证UPDATE结果
        parser = new Parser("SELECT * FROM accounts WHERE id = 2");
        stmt = parser.parse();
        result = executor.execute(stmt);
        Object balance = result.getRecords().get(0).getValues().get("balance");
        assert balance.equals(2500) : "Bob的balance应该是2500";
        System.out.println("  ✓ 验证：Bob的balance已更新");

        // 验证DELETE结果
        parser = new Parser("SELECT * FROM accounts WHERE id = 3");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.getRecords().size() == 0 : "Charlie的记录应该被删除";
        System.out.println("  ✓ 验证：Charlie的记录已删除");
    }

    private static void testAutoCommitMode(Executor executor) throws Exception {
        System.out.println("\n【测试5】自动提交模式");

        // 在没有BEGIN的情况下直接INSERT（自动提交）
        Parser parser = new Parser("INSERT INTO accounts VALUES (6, 'Frank', 6000)");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        assert result.isSuccess() : "自动提交模式INSERT应该成功";
        System.out.println("  ✓ 自动提交模式插入成功");

        // 立即查询验证（应该能查到）
        parser = new Parser("SELECT * FROM accounts WHERE id = 6");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.getRecords().size() == 1 : "自动提交的数据应该立即可见";
        System.out.println("  ✓ 验证：自动提交的数据立即可见");

        // 自动提交模式下的UPDATE
        parser = new Parser("UPDATE accounts SET balance = 7000 WHERE id = 6");
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "自动提交模式UPDATE应该成功";

        parser = new Parser("SELECT * FROM accounts WHERE id = 6");
        stmt = parser.parse();
        result = executor.execute(stmt);
        Object balance = result.getRecords().get(0).getValues().get("balance");
        assert balance.equals(7000) : "balance应该是7000";
        System.out.println("  ✓ 验证：自动提交的UPDATE已生效");
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
