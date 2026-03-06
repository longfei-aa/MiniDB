# MiniDB 项目结构说明

## 📁 目录组织

```
MiniDB/
├── README.md                          # 📖 项目主文档
├── docs/                              # 📚 技术文档目录 ✨ NEW
│   ├── README.md                     # 文档索引
│   └── 存储引擎更新计划.md              # Week 5-7 Buffer Pool改造计划
├── src/
│   ├── minidb.txt                    # 项目规划（Week 1-4）
│   └── main/java/com/minidb/
│       ├── MiniDB.java               # 主数据库类
│       ├── common/                   # 公共模块
│       ├── storage/                  # 存储引擎（Page, BufferPool, DiskManager）
│       ├── index/                    # 索引模块（B+树）
│       ├── sql/                      # SQL解析器
│       ├── executor/                 # 执行引擎
│       ├── transaction/              # 事务管理（Week 3）
│       ├── mvcc/                     # MVCC模块（Week 3）
│       ├── log/                      # 日志系统（Redo/Undo Log）
│       ├── lock/                     # 锁机制（Week 4）
│       ├── performance/              # 性能监控（Week 4）
│       └── test/                     # 测试模块
├── data/                             # 数据文件目录（.db文件、日志文件）
└── out/                              # 编译输出目录
```

## 📚 文档说明

### docs/ 目录
此目录存放所有项目相关的技术文档、设计文档和开发计划。

**当前文档**:
- `README.md` - 文档索引和导航
- `存储引擎更新计划.md` - Week 5-7的Buffer Pool改造详细计划

**规划文档**（待添加）:
- 架构设计文档
- API文档
- 性能测试报告
- 开发规范

### 文档访问路径

- 从IDE查看: 直接打开 `docs/` 目录下的markdown文件
- 从GitHub查看: 点击仓库中的 `docs/` 文件夹
- 从命令行查看: `cd docs && ls`

---

**最后更新**: 2025-12-20
