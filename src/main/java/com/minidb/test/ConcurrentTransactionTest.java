package com.minidb.test;

import com.minidb.common.Config;
import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 多会话并发事务测试。
 *
 * 当前目标：
 * 1. 不同 session 能持有各自事务上下文
 * 2. 同一行更新发生锁等待，而不是立即失败
 * 3. 前一个事务提交后，等待中的事务可以继续执行
 */
public class ConcurrentTransactionTest {

    public static void main(String[] args) throws Exception {
        System.out.println("========== Concurrent Transaction Test ==========\n");

        cleanupDataDir();

        DiskManager diskManager = new DiskManager("concurrent_test.db");
        BufferPool bufferPool = new BufferPool(diskManager, 50);
        Executor executor = new Executor(bufferPool, "concurrent_test");

        execute(executor, "ddl", "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)");
        execute(executor, "ddl", "INSERT INTO accounts VALUES (1, 100)");

        execute(executor, "s1", "BEGIN");
        execute(executor, "s1", "UPDATE accounts SET balance = 150 WHERE id = 1");

        CountDownLatch session2Started = new CountDownLatch(1);
        AtomicReference<ExecutionResult> session2UpdateResult = new AtomicReference<>();
        AtomicReference<Throwable> session2Failure = new AtomicReference<>();

        Thread t = new Thread(() -> {
            try {
                execute(executor, "s2", "BEGIN");
                session2Started.countDown();
                session2UpdateResult.set(execute(executor, "s2", "UPDATE accounts SET balance = 200 WHERE id = 1"));
                execute(executor, "s2", "COMMIT");
            } catch (Throwable t1) {
                session2Failure.set(t1);
            }
        });
        t.start();

        session2Started.await();
        Thread.sleep(200);
        assert t.isAlive() : "session2 应在等待 session1 释放锁";

        execute(executor, "s1", "COMMIT");
        t.join();

        assert session2Failure.get() == null : "session2 不应抛异常: " + session2Failure.get();
        assert session2UpdateResult.get() != null && session2UpdateResult.get().isSuccess()
            : "session2 更新应在等待后成功";

        ExecutionResult finalResult = execute(executor, "reader", "SELECT * FROM accounts WHERE id = 1");
        assert finalResult.isSuccess() : "最终查询应成功";
        assert finalResult.getRecords().size() == 1 : "应返回1条记录";
        Object balance = finalResult.getRecords().get(0).getValues().get("balance");
        assert Integer.valueOf(200).equals(balance) : "最终余额应为 200";

        executor.close();
        bufferPool.shutdown();
        diskManager.close();

        System.out.println("  ✓ multi-session transaction context works");
        System.out.println("  ✓ lock wait allows session2 to continue after session1 commit");
        System.out.println("  ✓ final committed balance = " + balance);
        System.out.println("\n========== Concurrent Transaction Test Passed ==========");
    }

    private static ExecutionResult execute(Executor executor, String sessionId, String sql) throws Exception {
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        return executor.execute(sessionId, stmt);
    }

    private static void cleanupDataDir() {
        File dataDir = new File(Config.DATA_DIR);
        if (!dataDir.exists()) {
            return;
        }
        File[] files = dataDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            deleteRecursively(file);
        }
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
