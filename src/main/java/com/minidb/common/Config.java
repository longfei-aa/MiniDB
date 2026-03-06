package com.minidb.common;

/**
 * 全局配置类
 */
public class Config {
    // 页面大小（8KB，与InnoDB一致）
    public static final int PAGE_SIZE = 8192;

    // Buffer Pool大小（页面数量）
    public static final int BUFFER_POOL_SIZE = 1024;

    // B+树节点最大键值对数量
    public static final int BTREE_ORDER = 128;

    // 数据文件存储路径
    public static final String DATA_DIR = "data/";

    // 默认表空间文件
    public static final String DEFAULT_TABLESPACE = "minidb.ibd";
}
