package com.minidb.test;

import com.minidb.common.Column;
import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;
import com.minidb.common.Config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 基本 CRUD 功能测试
 */
public class BasicCRUDTest {

    private static BufferPool bufferPool;
    private static Executor executor;
    private static DiskManager diskManager;

    public static void main(String[] args) {
        try {
            setup();

            System.out.println("========================================");
            System.out.println("开始测试基本 CRUD 功能");
            System.out.println("========================================\n");

            testCreateTable();
            testInsert();
            testSelect();
            testUpdate();
            testDelete();
            testFullTableScan();

            System.out.println("\n========================================");
            System.out.println("所有测试通过！");
            System.out.println("========================================");

            cleanup();

        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setup() throws Exception {
        System.out.println("初始化测试环境...");

        // 清理测试数据目录
        Path dataPath = Paths.get(Config.DATA_DIR);
        if (Files.exists(dataPath)) {
            Files.walk(dataPath)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception e) {
                        // ignore
                    }
                });
        }
        Files.createDirectories(dataPath);

        // 初始化组件
        diskManager = new DiskManager("test");  // DiskManager 会自动添加 DATA_DIR 前缀
        bufferPool = new BufferPool(diskManager, Config.BUFFER_POOL_SIZE);
        executor = new Executor(bufferPool);

        System.out.println("测试环境初始化完成\n");
    }

    private static void cleanup() throws Exception {
        System.out.println("\n清理测试环境...");
        bufferPool.flushAllPages();
        diskManager.close();
        System.out.println("清理完成");
    }

    private static void testCreateTable() throws Exception {
        System.out.println("【测试 1】CREATE TABLE");

        Parser parser = new Parser("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        if (result.isSuccess()) {
            System.out.println("✓ 创建表成功: " + result.getMessage());
        } else {
            throw new Exception("创建表失败: " + result.getMessage());
        }
        System.out.println();
    }

    private static void testInsert() throws Exception {
        System.out.println("【测试 2】INSERT");

        String[] insertSqls = {
            "INSERT INTO users VALUES (1, 'Alice', 25)",
            "INSERT INTO users VALUES (2, 'Bob', 30)",
            "INSERT INTO users VALUES (3, 'Charlie', 35)"
        };

        for (String sql : insertSqls) {
            Parser parser = new Parser(sql);
            Statement stmt = parser.parse();
            ExecutionResult result = executor.execute(stmt);

            if (result.isSuccess()) {
                System.out.println("✓ 插入成功: " + result.getMessage());
            } else {
                throw new Exception("插入失败: " + result.getMessage());
            }
        }
        System.out.println();
    }

    private static void testSelect() throws Exception {
        System.out.println("【测试 3】SELECT (索引查询)");

        Parser parser = new Parser("SELECT * FROM users WHERE id = 2");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        if (result.isSuccess() && result.getRecords() != null && result.getRecords().size() == 1) {
            System.out.println("✓ 查询成功，找到 " + result.getRecords().size() + " 条记录");
            System.out.println("  记录: " + result.getRecords().get(0));
        } else {
            throw new Exception("查询失败或结果不正确");
        }
        System.out.println();
    }

    private static void testUpdate() throws Exception {
        System.out.println("【测试 4】UPDATE");

        // 更新记录
        Parser parser = new Parser("UPDATE users SET id = 10 WHERE id = 2");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        if (result.isSuccess()) {
            System.out.println("✓ 更新成功: " + result.getMessage());

            // 验证更新
            parser = new Parser("SELECT * FROM users WHERE id = 10");
            stmt = parser.parse();
            result = executor.execute(stmt);

            if (result.isSuccess() && result.getRecords() != null && result.getRecords().size() == 1) {
                System.out.println("✓ 验证更新成功，新记录: " + result.getRecords().get(0));
            } else {
                throw new Exception("更新验证失败");
            }
        } else {
            throw new Exception("更新失败: " + result.getMessage());
        }
        System.out.println();
    }

    private static void testDelete() throws Exception {
        System.out.println("【测试 5】DELETE");

        // 删除记录
        Parser parser = new Parser("DELETE FROM users WHERE id = 3");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        if (result.isSuccess()) {
            System.out.println("✓ 删除成功: " + result.getMessage());

            // 验证删除
            parser = new Parser("SELECT * FROM users WHERE id = 3");
            stmt = parser.parse();
            result = executor.execute(stmt);

            if (result.isSuccess() && (result.getRecords() == null || result.getRecords().size() == 0)) {
                System.out.println("✓ 验证删除成功，记录已不存在");
            } else {
                throw new Exception("删除验证失败");
            }
        } else {
            throw new Exception("删除失败: " + result.getMessage());
        }
        System.out.println();
    }

    private static void testFullTableScan() throws Exception {
        System.out.println("【测试 6】全表扫描");

        Parser parser = new Parser("SELECT * FROM users");
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        if (result.isSuccess() && result.getRecords() != null) {
            System.out.println("✓ 全表扫描成功，找到 " + result.getRecords().size() + " 条记录");
            for (int i = 0; i < result.getRecords().size(); i++) {
                System.out.println("  记录 " + (i + 1) + ": " + result.getRecords().get(i));
            }
        } else {
            throw new Exception("全表扫描失败");
        }
        System.out.println();
    }
}
