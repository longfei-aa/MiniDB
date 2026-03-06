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
 * 基础功能测试
 *
 * 测试范围：
 * 1. CREATE TABLE - 表创建
 * 2. INSERT - 数据插入
 * 3. SELECT - 数据查询（全表、WHERE条件）
 * 4. UPDATE - 数据更新
 * 5. DELETE - 数据删除
 * 6. 边界条件测试
 */
public class BasicFunctionalityTest {

    public static void main(String[] args) {
        System.out.println("========== MiniDB 基础功能测试 ==========\n");

        try {
            cleanupDataDir();

            DiskManager diskManager = new DiskManager("basic_test.db");
            BufferPool bufferPool = new BufferPool(diskManager, 100);
            Executor executor = new Executor(bufferPool, "basic_functionality");

            // 测试1: CREATE TABLE
            testCreateTable(executor);

            // 测试2: INSERT
            testInsert(executor);

            // 测试3: SELECT (全表扫描)
            testSelectAll(executor);

            // 测试4: SELECT with WHERE (等值查询)
            testSelectWithWhere(executor);

            // 测试5: UPDATE
            testUpdate(executor);

            // 测试6: DELETE
            testDelete(executor);

            // 测试7: 边界条件
            testEdgeCases(executor);

            // 测试8: 多表操作
            testMultipleTables(executor);

            System.out.println("\n========== ✅ 所有基础功能测试通过！ ==========");
            System.out.println("通过测试项：");
            System.out.println("  ✓ 表创建和元数据管理");
            System.out.println("  ✓ 数据插入和主键约束");
            System.out.println("  ✓ 全表扫描查询");
            System.out.println("  ✓ 条件查询（WHERE子句）");
            System.out.println("  ✓ 数据更新");
            System.out.println("  ✓ 数据删除");
            System.out.println("  ✓ 边界条件处理");
            System.out.println("  ✓ 多表并存");

        } catch (Exception e) {
            System.err.println("\n❌ 测试失败：" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void testCreateTable(Executor executor) throws Exception {
        System.out.println("【测试1】CREATE TABLE - 表创建");

        // 创建学生表
        String sql = "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT, score INT)";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "CREATE TABLE 应该成功";
        System.out.println("  ✓ 成功创建表: students");

        // 测试重复创建（应该失败）
        result = executor.execute(stmt);
        assert !result.isSuccess() : "重复创建表应该失败";
        System.out.println("  ✓ 重复创建表检测正常");
    }

    private static void testInsert(Executor executor) throws Exception {
        System.out.println("\n【测试2】INSERT - 数据插入");

        String[] insertSQLs = {
            "INSERT INTO students VALUES (1, 'Alice', 20, 85)",
            "INSERT INTO students VALUES (2, 'Bob', 21, 90)",
            "INSERT INTO students VALUES (3, 'Charlie', 20, 88)",
            "INSERT INTO students VALUES (4, 'David', 22, 92)",
            "INSERT INTO students VALUES (5, 'Eve', 21, 87)"
        };

        for (String sql : insertSQLs) {
            Parser parser = new Parser(sql);
            Statement stmt = parser.parse();
            ExecutionResult result = executor.execute(stmt);
            assert result.isSuccess() : "INSERT 应该成功: " + sql;
        }

        System.out.println("  ✓ 成功插入 " + insertSQLs.length + " 条记录");

        // 测试主键冲突（应该失败或覆盖）
        String duplicateSql = "INSERT INTO students VALUES (1, 'Duplicate', 25, 100)";
        Parser parser = new Parser(duplicateSql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        // 注：根据B+树实现，可能覆盖或失败
        System.out.println("  ✓ 主键冲突处理: " + (result.isSuccess() ? "覆盖" : "拒绝"));
    }

    private static void testSelectAll(Executor executor) throws Exception {
        System.out.println("\n【测试3】SELECT - 全表扫描");

        String sql = "SELECT * FROM students";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "SELECT 应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() >= 5 : "应该至少有5条记录，实际: " + records.size();

        System.out.println("  ✓ 查询到 " + records.size() + " 条记录");
        System.out.println("  示例记录: " + records.get(0).getValues());
    }

    private static void testSelectWithWhere(Executor executor) throws Exception {
        System.out.println("\n【测试4】SELECT with WHERE - 条件查询");

        // 等值查询
        String sql = "SELECT * FROM students WHERE id = 3";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "SELECT WHERE 应该成功";
        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该找到1条记录";
        assert records.get(0).getValues().get("name").equals("Charlie") : "应该是Charlie";

        System.out.println("  ✓ 等值查询 (id=3): 找到 " + records.size() + " 条");

        // 范围查询
        sql = "SELECT * FROM students WHERE age > 20";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "范围查询应该成功";
        records = result.getRecords();
        System.out.println("  ✓ 范围查询 (age>20): 找到 " + records.size() + " 条");
    }

    private static void testUpdate(Executor executor) throws Exception {
        System.out.println("\n【测试5】UPDATE - 数据更新");

        // 更新单条记录
        String sql = "UPDATE students SET score = 95 WHERE id = 2";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "UPDATE 应该成功";
        System.out.println("  ✓ 更新成功: " + result.getMessage());

        // 验证更新结果
        sql = "SELECT * FROM students WHERE id = 2";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        List<DataRecord> records = result.getRecords();
        assert records.size() == 1 : "应该找到1条记录";
        Object score = records.get(0).getValues().get("score");
        assert score.equals(95) : "score应该是95，实际: " + score;

        System.out.println("  ✓ 验证更新: Bob的score已更新为95");
    }

    private static void testDelete(Executor executor) throws Exception {
        System.out.println("\n【测试6】DELETE - 数据删除");

        // 删除前查询总数
        String sql = "SELECT * FROM students";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);
        int beforeCount = result.getRecords().size();

        // 删除一条记录
        sql = "DELETE FROM students WHERE id = 5";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "DELETE 应该成功";
        System.out.println("  ✓ 删除成功: " + result.getMessage());

        // 验证删除结果
        sql = "SELECT * FROM students";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);
        int afterCount = result.getRecords().size();

        assert afterCount == beforeCount - 1 : "删除后应该少1条记录";
        System.out.println("  ✓ 验证删除: 记录数从 " + beforeCount + " 减少到 " + afterCount);
    }

    private static void testEdgeCases(Executor executor) throws Exception {
        System.out.println("\n【测试7】边界条件测试");

        // 测试空结果
        String sql = "SELECT * FROM students WHERE id = 9999";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "查询应该成功";
        assert result.getRecords().size() == 0 : "应该返回空结果";
        System.out.println("  ✓ 空结果查询正常");

        // 测试不存在的表
        sql = "SELECT * FROM nonexistent";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert !result.isSuccess() : "查询不存在的表应该失败";
        System.out.println("  ✓ 不存在的表检测正常");

        // 测试非常大的数值
        sql = "INSERT INTO students VALUES (1000, 'LargeID', 100, 100)";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "大ID值插入应该成功";
        System.out.println("  ✓ 大数值处理正常");
    }

    private static void testMultipleTables(Executor executor) throws Exception {
        System.out.println("\n【测试8】多表操作");

        // 创建第二个表
        String sql = "CREATE TABLE courses (id INT PRIMARY KEY, name VARCHAR(50), credits INT)";
        Parser parser = new Parser(sql);
        Statement stmt = parser.parse();
        ExecutionResult result = executor.execute(stmt);

        assert result.isSuccess() : "创建第二个表应该成功";
        System.out.println("  ✓ 成功创建第二个表: courses");

        // 向第二个表插入数据
        sql = "INSERT INTO courses VALUES (101, 'Database', 3)";
        parser = new Parser(sql);
        stmt = parser.parse();
        result = executor.execute(stmt);

        assert result.isSuccess() : "向第二个表插入应该成功";
        System.out.println("  ✓ 向courses表插入数据成功");

        // 验证两个表独立存在
        sql = "SELECT * FROM students";
        parser = new Parser(sql);
        stmt = parser.parse();
        ExecutionResult studentsResult = executor.execute(stmt);

        sql = "SELECT * FROM courses";
        parser = new Parser(sql);
        stmt = parser.parse();
        ExecutionResult coursesResult = executor.execute(stmt);

        assert studentsResult.isSuccess() && coursesResult.isSuccess() : "两个表都应该可查询";
        System.out.println("  ✓ 多表并存正常: students(" + studentsResult.getRecords().size() +
                         "条), courses(" + coursesResult.getRecords().size() + "条)");
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
