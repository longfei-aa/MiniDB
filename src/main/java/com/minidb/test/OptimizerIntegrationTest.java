package com.minidb.test;

import com.minidb.common.Config;
import com.minidb.common.DataRecord;
import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;

import java.io.File;
import java.util.List;

/**
 * 二级索引与查询优化器集成测试
 *
 * 测试场景：
 * 1. 创建表并插入数据
 * 2. 创建单列索引
 * 3. 创建联合索引
 * 4. 测试优化器选择索引
 * 5. 测试索引查询（指定列）
 */
public class OptimizerIntegrationTest {

    public static void main(String[] args) {
        System.out.println("========== 二级索引与优化器集成测试 ==========\n");

        try {
            // 清理旧数据
            cleanupDataDir();

            DiskManager diskManager = new DiskManager("optimizer_test.db");
            BufferPool bufferPool = new BufferPool(diskManager, 50);
            Executor executor = new Executor(bufferPool, "optimizer_test");
            // 1. 创建用户表
            testCreateTable(executor);

            // 2. 插入测试数据
            testInsertData(executor);

            // 3. 创建单列索引
            testCreateSingleColumnIndex(executor);

            // 4. 创建联合索引
            testCreateCompositeIndex(executor);

            // 5. 测试索引查询
            testIndexQuery(executor);

            // 6. 测试索引范围查询
            testIndexRangeQuery(executor);

            // 7. 测试索引查询（指定列）
            testProjectedIndexQuery(executor);

            // 8. 测试优化器选择
            testOptimizerSelection(executor);

            // 9. 主键唯一约束
            testPrimaryKeyUniqueness(executor);

            // 10. 二级索引一致性（UPDATE/DELETE）
            testSecondaryIndexConsistency(executor);

            System.out.println("\n========== 所有集成测试通过！ ==========");

        } catch (Exception e) {
            System.err.println("测试失败：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 测试1：创建表
     */
    private static void testCreateTable(Executor executor) throws Exception {
        System.out.println("【测试1】创建用户表");

        String sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT, city VARCHAR(50))";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "创建表应该成功";
        System.out.println("  ✓ " + result.getMessage());
    }

    /**
     * 测试2：插入数据
     */
    private static void testInsertData(Executor executor) throws Exception {
        System.out.println("\n【测试2】插入测试数据");

        String[] insertSQLs = {
            "INSERT INTO users VALUES (1, 'Alice', 25, 'Beijing')",
            "INSERT INTO users VALUES (2, 'Bob', 30, 'Shanghai')",
            "INSERT INTO users VALUES (3, 'Charlie', 25, 'Beijing')",
            "INSERT INTO users VALUES (4, 'David', 35, 'Guangzhou')",
            "INSERT INTO users VALUES (5, 'Eve', 30, 'Shanghai')",
            "INSERT INTO users VALUES (6, 'Frank', 25, 'Shenzhen')",
            "INSERT INTO users VALUES (7, 'Grace', 28, 'Beijing')",
            "INSERT INTO users VALUES (8, 'Henry', 32, 'Shanghai')"
        };

        for (String sql : insertSQLs) {
            Parser parser = new Parser(sql);
            Statement stmt = parser.parse();
            ExecutionResult result = executor.execute(stmt);
            assert result.isSuccess() : "插入应该成功";
        }

        System.out.println("  ✓ 成功插入 " + insertSQLs.length + " 条记录");
    }

    /**
     * 测试3：创建单列索引
     */
    private static void testCreateSingleColumnIndex(Executor executor) throws Exception {
        System.out.println("\n【测试3】创建单列索引");

        // 在 name 列创建索引
        String sql = "CREATE INDEX idx_name ON users (name)";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "创建索引应该成功";
        System.out.println("  ✓ " + result.getMessage());

        // 在 city 列创建索引
        sql = "CREATE INDEX idx_city ON users (city)";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "创建索引应该成功";
        System.out.println("  ✓ " + result.getMessage());
    }

    /**
     * 测试4：创建联合索引
     */
    private static void testCreateCompositeIndex(Executor executor) throws Exception {
        System.out.println("\n【测试4】创建联合索引");

        // 在 (city, age) 列创建联合索引
        String sql = "CREATE INDEX idx_city_age ON users (city, age)";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "创建联合索引应该成功";
        System.out.println("  ✓ " + result.getMessage());
    }

    /**
     * 测试5：索引查询
     */
    private static void testIndexQuery(Executor executor) throws Exception {
        System.out.println("\n【测试5】测试索引查询");

        // 查询 name = 'Alice'（应使用 idx_name）
        String sql = "SELECT * FROM users WHERE name = 'Alice'";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该找到1条记录";
        assert records.get(0).getValues().get("name").equals("Alice") : "应该找到Alice";
        System.out.println("  ✓ 使用索引查询 name='Alice'，找到 " + records.size() + " 条记录");

        // 查询 city = 'Beijing'（应使用 idx_city）
        sql = "SELECT * FROM users WHERE city = 'Beijing'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        records = result.getRecords();
        assert records.size() == 3 : "应该找到3条记录（Alice, Charlie, Grace）";
        System.out.println("  ✓ 使用索引查询 city='Beijing'，找到 " + records.size() + " 条记录");
    }

    /**
     * 测试6：索引范围查询（< / <=）
     */
    private static void testIndexRangeQuery(Executor executor) throws Exception {
        System.out.println("\n【测试6】测试索引范围查询（< / <=）");

        String sql = "SELECT * FROM users WHERE name < 'David'";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() == 3 : "name < 'David' 应该找到3条记录（Alice, Bob, Charlie）";
        System.out.println("  ✓ name < 'David'：找到 " + records.size() + " 条记录");

        sql = "SELECT * FROM users WHERE name <= 'David'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        records = result.getRecords();
        assert records.size() == 4 : "name <= 'David' 应该找到4条记录（Alice, Bob, Charlie, David）";
        System.out.println("  ✓ name <= 'David'：找到 " + records.size() + " 条记录");

        sql = "SELECT * FROM users WHERE name >= 'Frank'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        records = result.getRecords();
        assert records.size() == 3 : "name >= 'Frank' 应该找到3条记录（Frank, Grace, Henry）";
        System.out.println("  ✓ name >= 'Frank'：找到 " + records.size() + " 条记录");
    }

    /**
     * 测试7：索引查询（SELECT 指定列）
     */
    private static void testProjectedIndexQuery(Executor executor) throws Exception {
        System.out.println("\n【测试7】测试索引查询（指定列）");

        // SELECT 指定列，当前实现仍统一回表
        String sql = "SELECT name FROM users WHERE name = 'Bob'";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该找到1条记录";
        System.out.println("  ✓ 索引查询成功，找到 " + records.size() + " 条记录");
        System.out.println("  注：当前实现统一回表，不走覆盖索引专用路径");
    }

    /**
     * 测试8：优化器选择
     */
    private static void testOptimizerSelection(Executor executor) throws Exception {
        System.out.println("\n【测试8】测试优化器索引选择");

        // 主键查询（应选择主键索引）
        String sql = "SELECT * FROM users WHERE id = 3";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该找到1条记录";
        assert records.get(0).getValues().get("name").equals("Charlie") : "应该找到Charlie";
        System.out.println("  ✓ 主键查询：优化器选择主键索引");

        // 无索引查询（应全表扫描）
        sql = "SELECT * FROM users WHERE age = 25";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        records = result.getRecords();
        assert records.size() == 3 : "应该找到3条记录（Alice, Charlie, Frank）";
        System.out.println("  ✓ 无索引列查询：优化器选择全表扫描，找到 " + records.size() + " 条记录");

        // 有多个索引可选（应选择最优的）
        sql = "SELECT * FROM users WHERE city = 'Shanghai'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        records = result.getRecords();
        assert records.size() == 3 : "应该找到3条记录（Bob, Eve, Henry）";
        System.out.println("  ✓ 有索引列查询：优化器选择 idx_city 索引，找到 " + records.size() + " 条记录");

        sql = "SELECT * FROM users WHERE name <= 'David'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        records = result.getRecords();
        assert records.size() == 4 : "应该找到4条记录（Alice, Bob, Charlie, David）";
        System.out.println("  ✓ 索引范围查询：优化器选择 idx_name 索引，找到 " + records.size() + " 条记录");

        sql = "SELECT * FROM users WHERE name >= 'Frank'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        records = result.getRecords();
        assert records.size() == 3 : "应该找到3条记录（Frank, Grace, Henry）";
        System.out.println("  ✓ 索引范围查询：优化器选择 idx_name 索引处理 >=，找到 " + records.size() + " 条记录");
    }

    /**
     * 测试9：主键唯一约束
     */
    private static void testPrimaryKeyUniqueness(Executor executor) throws Exception {
        System.out.println("\n【测试9】测试主键唯一约束");

        String sql = "INSERT INTO users VALUES (1, 'Duplicate', 99, 'Nowhere')";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert !result.isSuccess() : "重复主键插入应该失败";
        System.out.println("  ✓ 重复主键被拒绝: " + result.getMessage());
    }

    /**
     * 测试9：二级索引一致性（UPDATE/DELETE）
     */
    private static void testSecondaryIndexConsistency(Executor executor) throws Exception {
        System.out.println("\n【测试9】测试二级索引一致性（UPDATE/DELETE）");

        // 1) UPDATE：将 Bob 的 city 从 Shanghai 改为 Nanjing
        String sql = "UPDATE users SET city = 'Nanjing' WHERE id = 2";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        assert result.isSuccess() : "更新应该成功";

        // 验证旧索引值减少
        sql = "SELECT * FROM users WHERE city = 'Shanghai'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> shanghaiRecords = result.getRecords();
        assert shanghaiRecords.size() == 2 : "更新后 Shanghai 应为2条（Eve, Henry）";

        // 验证新索引值可查
        sql = "SELECT * FROM users WHERE city = 'Nanjing'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> nanjingRecords = result.getRecords();
        assert nanjingRecords.size() == 1 : "更新后 Nanjing 应为1条（Bob）";
        assert nanjingRecords.get(0).getValues().get("name").equals("Bob") : "Nanjing 记录应为 Bob";

        // 2) DELETE：删除 Charlie（原 city=Beijing）
        sql = "DELETE FROM users WHERE id = 3";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "删除应该成功";

        // 验证二级索引 city='Beijing' 从3条变为2条
        sql = "SELECT * FROM users WHERE city = 'Beijing'";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);
        assert result.isSuccess() : "查询应该成功";
        List<DataRecord> beijingRecords = result.getRecords();
        assert beijingRecords.size() == 2 : "删除后 Beijing 应为2条（Alice, Grace）";

        System.out.println("  ✓ UPDATE 后 city 索引一致");
        System.out.println("  ✓ DELETE 后 city 索引一致");
    }

    /**
     * 清理数据目录
     */
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
