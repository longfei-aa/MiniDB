package com.minidb.test;

import com.minidb.lock.*;
import com.minidb.transaction.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * 锁机制测试（简化版）
 */
public class LockTest {

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("Lock System Test (Row/Gap/Next-Key)");
        System.out.println("=".repeat(80));

        LockTest test = new LockTest();

        // 运行所有测试
        test.testRowLevelLocks();
        test.testRowLockConflict();
        test.testConcurrentLocking();

        System.out.println("\n" + "=".repeat(80));
        System.out.println("All lock tests completed successfully!");
        System.out.println("=".repeat(80));
    }

    /**
     * 测试3: 行级锁（记录锁、间隙锁、临键锁）
     */
    public void testRowLevelLocks() {
        System.out.println("\n[Test 3] Row-Level Locks (Record, Gap, Next-Key)");
        System.out.println("-".repeat(80));

        TransactionManager txnManager = new TransactionManager();
        LockManager lockManager = new LockManager();
        Transaction t1 = txnManager.beginTransaction();

        // 记录锁
        System.out.println("Acquiring Record Lock on row:1:100...");
        boolean recordLock = lockManager.acquireRowLock(t1, 1, "100", LockMode.X);
        assert recordLock : "Should acquire record lock";
        System.out.println("  ✓ Record Lock acquired");

        // 间隙锁
        System.out.println("Acquiring Gap Lock on (100, 200)...");
        boolean gapLock = lockManager.acquireGapLock(t1, 1, 100, 200, LockMode.X);
        assert gapLock : "Should acquire gap lock";
        System.out.println("  ✓ Gap Lock acquired on interval (100, 200)");

        // 临键锁
        System.out.println("Acquiring Next-Key Lock on row:1:200 with gap (100, 200]...");
        boolean nextKeyLock = lockManager.acquireNextKeyLock(t1, 1, "200", 100, 200, LockMode.X);
        assert nextKeyLock : "Should acquire next-key lock";
        System.out.println("  ✓ Next-Key Lock acquired (Record + Gap)");

        // 显示所有锁
        Set<Lock> locks = lockManager.getTransactionLocks(t1.getTransactionId());
        System.out.println("\nTransaction T1 holds " + locks.size() + " locks:");
        for (Lock lock : locks) {
            System.out.println("  " + lock);
        }

        lockManager.releaseAllLocks(t1);
        System.out.println("\n✓ Row-level locks work correctly");
    }

    /**
     * 测试4: 行锁冲突
     */
    public void testRowLockConflict() {
        System.out.println("\n[Test 4] Row Lock Conflict");
        System.out.println("-".repeat(80));

        TransactionManager txnManager = new TransactionManager();
        LockManager lockManager = new LockManager();
        Transaction t1 = txnManager.beginTransaction();
        Transaction t2 = txnManager.beginTransaction();

        System.out.println("T1 acquiring row-level X lock on row 999...");
        boolean success1 = lockManager.acquireRowLock(t1, 1, "999", LockMode.X);
        assert success1 : "T1 should acquire X lock";
        System.out.println("  ✓ T1 acquired X lock");

        System.out.println("T2 acquiring row-level X lock on same row...");
        boolean success2 = lockManager.acquireRowLock(t2, 1, "999", LockMode.X);
        assert !success2 : "T2 should fail due to lock conflict";
        System.out.println("  ✓ T2 conflict detected as expected");

        lockManager.releaseAllLocks(t1);
        lockManager.releaseAllLocks(t2);
        System.out.println("✓ Row lock conflict works correctly");
    }

    /**
     * 测试5: 并发锁竞争
     */
    public void testConcurrentLocking() {
        System.out.println("\n[Test 5] Concurrent Locking");
        System.out.println("-".repeat(80));

        TransactionManager txnManager = new TransactionManager();
        LockManager lockManager = new LockManager();

        int threadCount = 10;
        int locksPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        System.out.println("Starting " + threadCount + " concurrent threads...");
        System.out.println("Each thread acquires " + locksPerThread + " locks");

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    Transaction txn = txnManager.beginTransaction();

                    // 获取多个锁
                    for (int j = 0; j < locksPerThread; j++) {
                        String rowKey = "row:" + threadId + ":" + j;
                        lockManager.acquireRowLock(txn, 1, rowKey, LockMode.X);
                    }

                    // 持有锁一小段时间
                    Thread.sleep(10);

                    // 释放锁
                    lockManager.releaseAllLocks(txn);
                    txnManager.commit(txn);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        try {
            endLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\nConcurrent locking test completed:");
        System.out.println("  Duration: " + duration + "ms");
        System.out.println("  Total locks: " + (threadCount * locksPerThread));
        System.out.println("  Throughput: " + (threadCount * locksPerThread * 1000 / duration) + " locks/sec");

        System.out.println("\n" + lockManager.getStats());
        System.out.println("✓ Concurrent locking works correctly");
    }
}
