package com.minidb;

import com.minidb.executor.ExecutionResult;
import com.minidb.executor.Executor;
import com.minidb.sql.Parser;
import com.minidb.sql.Statement;
import com.minidb.storage.BufferPool;
import com.minidb.storage.DiskManager;
import java.io.IOException;

/**
 * MiniDB - 主数据库类
 */
public class MiniDB {
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final Executor executor;
    private final String dbName;
    private final boolean verbose;

    public MiniDB(String dbName) throws IOException {
        this(dbName, true);
    }

    public MiniDB(String dbName, boolean verbose) throws IOException {
        this.dbName = dbName;
        this.verbose = verbose;
        this.diskManager = new DiskManager(dbName);
        this.bufferPool = new BufferPool(diskManager);
        this.executor = new Executor(bufferPool, dbName);

        if (this.verbose) {
            System.out.println("MiniDB initialized: " + dbName);
            System.out.println("Data file: " + diskManager.getDataFilePath());
        }
    }

    /**
     * 执行SQL语句
     */
    public ExecutionResult executeSQL(String sql) {
        return executeSQL("default", sql);
    }

    /**
     * 按会话执行 SQL 语句。不同 session 维护各自的事务上下文。
     */
    public ExecutionResult executeSQL(String sessionId, String sql) {
        try {
            if (verbose) {
                System.out.println("\n[SQL] " + sql);
            }

            Parser parser = new Parser(sql);
            Statement statement = parser.parse();

            ExecutionResult result = executor.execute(sessionId, statement);
            if (verbose) {
                System.out.println("[Result] " + result);
            }

            return result;
        } catch (Exception e) {
            String errorMsg = "Error executing SQL: " + e.getMessage();
            if (verbose) {
                System.out.println("[Error] " + errorMsg);
            }
            return new ExecutionResult(false, errorMsg);
        }
    }

    /**
     * 显示统计信息
     */
    public void showStats() {
        System.out.println("\n=== Database Statistics ===");
        System.out.println("Database: " + dbName);
        System.out.println("Tables: " + executor.getTables().size());
        System.out.println(bufferPool.getStats());
        System.out.println("Total Pages: " + diskManager.getPageCount());
    }

    /**
     * 关闭数据库
     */
    public void close() throws IOException {
        if (verbose) {
            System.out.println("\nClosing MiniDB...");
        }
        executor.close();
        bufferPool.shutdown();
        diskManager.close();
        if (verbose) {
            System.out.println("MiniDB closed successfully");
        }
    }

    public static void main(String[] args) {
        try {
            // 创建数据库实例
            MiniDB db = new MiniDB("test_db");

            // 创建表
            db.executeSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");

            // 插入数据
            db.executeSQL("INSERT INTO users VALUES (1, 'Alice', 25)");
            db.executeSQL("INSERT INTO users VALUES (2, 'Bob', 30)");
            db.executeSQL("INSERT INTO users VALUES (3, 'Charlie', 35)");

            // 查询数据
            db.executeSQL("SELECT * FROM users WHERE id = 1");
            db.executeSQL("SELECT * FROM users WHERE id = 2");

            // 显示统计信息
            db.showStats();

            // 关闭数据库
            db.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
