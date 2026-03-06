package com.minidb.test;

/**
 * 面试版最小测试套件运行器
 * 仅保留核心能力验证：
 * 1. SQL 基础
 * 2. MVCC + 事务
 * 3. 索引 + 优化器
 * 4. Redo 恢复
 */
public class AllTestsRunner {

    public static void main(String[] args) {
        System.out.println("==== MiniDB Interview Suite ====");

        int total = 5;
        int passed = 0;
        long start = System.currentTimeMillis();

        passed += run("BasicFunctionalityTest", () -> BasicFunctionalityTest.main(new String[]{}));
        passed += run("MVCCTransactionTest", () -> MVCCTransactionTest.main(new String[]{}));
        passed += run("OptimizerIntegrationTest", () -> OptimizerIntegrationTest.main(new String[]{}));
        passed += run("RedoRecoveryTest", () -> RedoRecoveryTest.main(new String[]{}));
        passed += run("ConcurrentTransactionTest", () -> {
            try {
                ConcurrentTransactionTest.main(new String[]{});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        long cost = System.currentTimeMillis() - start;
        System.out.println();
        System.out.println("Summary: passed=" + passed + "/" + total + ", cost=" + cost + "ms");
        if (passed != total) {
            System.exit(1);
        }
    }

    private static int run(String name, Runnable task) {
        long begin = System.currentTimeMillis();
        try {
            System.out.println("[RUN] " + name);
            task.run();
            System.out.println("[OK ] " + name + " (" + (System.currentTimeMillis() - begin) + "ms)");
            return 1;
        } catch (Throwable t) {
            System.err.println("[ERR] " + name + ": " + t.getMessage());
            t.printStackTrace();
            return 0;
        }
    }
}
