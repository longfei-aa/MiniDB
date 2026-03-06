package com.minidb.index;

import com.minidb.common.Config;
import com.minidb.common.DataRecord;
import com.minidb.storage.BufferPool;
import com.minidb.storage.Page;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * B+树索引实现
 * 面试版定位：通过整树读写锁提供最小并发安全。
 *
 * 功能：
 * 1. 插入键值对
 * 2. 查找键值
 * 3. 删除键值
 * 4. 范围查询
 * 5. 节点分裂和合并
 */
public class BPlusTree {
    private final BufferPool bufferPool;
    private int rootPageId;
    private final int order;  // B+树的阶
    private final ReentrantReadWriteLock treeLock;

    public BPlusTree(BufferPool bufferPool) throws IOException {
        this(bufferPool, Config.BTREE_ORDER);
    }

    public BPlusTree(BufferPool bufferPool, int order) throws IOException {
        this(bufferPool, order, -1);
    }

    /**
     * 基于已有根页加载 B+Tree；existingRootPageId <= 0 时创建新树。
     */
    public BPlusTree(BufferPool bufferPool, int order, int existingRootPageId) throws IOException {
        this.bufferPool = bufferPool;
        this.order = order;
        this.treeLock = new ReentrantReadWriteLock();

        if (existingRootPageId > 0) {
            this.rootPageId = resolveCurrentRootPageId(existingRootPageId);
        } else {
            // 创建根节点
            Page rootPage = bufferPool.newPage();
            this.rootPageId = rootPage.getPageId();
            rootPage.setPageType(Page.PageType.INDEX_PAGE);

            // 初始化为叶子节点
            BPlusTreeNode root = new BPlusTreeNode(rootPageId, true, order);
            serializeNode(root, rootPage);
            bufferPool.unpinPage(rootPageId, true);
        }
    }

    /**
     * 允许从“历史根页”恢复到“当前根页”：
     * 若 table_roots 元数据滞后，可沿 parent 指针向上追溯到真正根节点。
     */
    private int resolveCurrentRootPageId(int seedPageId) throws IOException {
        int current = seedPageId;
        Set<Integer> visited = new HashSet<>();

        while (current > 0 && visited.add(current)) {
            Page page = bufferPool.fetchPage(current);
            int parentPageId;
            try {
                BPlusTreeNode node = deserializeNode(page);
                parentPageId = node.getParentPageId();
            } finally {
                bufferPool.unpinPage(current, false);
            }

            if (parentPageId <= 0 || parentPageId == current) {
                return current;
            }
            current = parentPageId;
        }

        throw new IOException("Failed to resolve current root page from seed page: " + seedPageId);
    }

    /**
     * 插入完整记录。
     *
     * @param key 主键
     * @param record 完整 DataRecord
     */
    public void insertRecord(int key, DataRecord record) throws IOException {
        treeLock.writeLock().lock();
        try {
            insertRecordInternal(key, record);
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    /**
     * 插入记录的内部实现
     */
    private void insertRecordInternal(int key, DataRecord record) throws IOException {
        InsertResult result = insertIntoNode(rootPageId, key, record);
        if (result.hasSplit()) {
            createNewRoot(result);
        }
    }

    /**
     * 查找完整记录（返回 DataRecord）
     */
    public DataRecord searchRecord(int key) throws IOException {
        treeLock.readLock().lock();
        try {
            return searchRecordInNode(rootPageId, key);
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * 在节点中查找记录
     */
    private DataRecord searchRecordInNode(int pageId, int key) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        try {
            if (node.isLeaf()) {
                int pos = node.findKeyPosition(key);
                // 叶子节点，返回 DataRecord
                if (pos < node.getKeys().size() && node.getKeys().get(pos) == key) {
                    return node.getRecords().get(pos);
                }
                return null;
            } else {
                // 内部节点，递归查找
                int childPageId = node.getChildren().get(findChildIndex(node, key));
                return searchRecordInNode(childPageId, key);
            }
        } finally {
            bufferPool.unpinPage(pageId, false);
        }
    }

    /**
     * 范围查询
     */
    public List<Integer> rangeSearch(int startKey, int endKey) throws IOException {
        treeLock.readLock().lock();
        try {
            List<Integer> results = new ArrayList<>();

            int leafPageId = findLeafNode(rootPageId, startKey);

            while (leafPageId != -1) {
                Page page = bufferPool.fetchPage(leafPageId);
                BPlusTreeNode leaf = deserializeNode(page);

                int nextLeafPageId = -1;

                for (int i = 0; i < leaf.getKeys().size(); i++) {
                    int key = leaf.getKeys().get(i);
                    if (key >= startKey && key <= endKey) {
                        results.add(key);
                    } else if (key > endKey) {
                        bufferPool.unpinPage(leafPageId, false);
                        return results;
                    }
                }

                nextLeafPageId = leaf.getNextLeafPageId();
                bufferPool.unpinPage(page.getPageId(), false);
                leafPageId = nextLeafPageId;
            }

            return results;
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * 找到包含指定键的叶子节点
     */
    private int findLeafNode(int pageId, int key) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        try {
            if (node.isLeaf()) {
                return pageId;
            } else {
                int childPageId = node.getChildren().get(findChildIndex(node, key));
                return findLeafNode(childPageId, key);
            }
        } finally {
            bufferPool.unpinPage(pageId, false);
        }
    }

    /**
     * 序列化节点到页面
     */
    private void serializeNode(BPlusTreeNode node, Page page) {
        page.getData().position(38);  // 跳过页面头部

        // 写入节点类型
        page.getData().put((byte) (node.isLeaf() ? 1 : 0));

        // 写入键值数量
        page.getData().putInt(node.getKeys().size());

        // 写入键值
        for (int key : node.getKeys()) {
            page.getData().putInt(key);
        }

        // 写入值或子节点指针
        if (node.isLeaf()) {
            // 叶子节点：序列化 DataRecord
            for (DataRecord record : node.getRecords()) {
                serializeDataRecord(page.getData(), record);
            }
            page.getData().putInt(node.getNextLeafPageId());
            page.getData().putInt(node.getPrevLeafPageId());
        } else {
            // 内部节点：写入子节点指针
            for (int child : node.getChildren()) {
                page.getData().putInt(child);
            }
        }

        page.getData().putInt(node.getParentPageId());
        page.markDirty();
    }

    /**
     * 序列化 DataRecord 到 ByteBuffer
     */
    private void serializeDataRecord(java.nio.ByteBuffer buffer, DataRecord record) {
        // 写入隐藏列
        buffer.putLong(record.getDbRowId());
        buffer.putLong(record.getDbTrxId());
        buffer.putLong(record.getDbRollPtr());
        buffer.put((byte) (record.isDeleted() ? 1 : 0));

        // 写入用户数据
        java.util.Map<String, Object> values = record.getValues();
        buffer.putInt(values.size());  // 字段数量

        for (java.util.Map.Entry<String, Object> entry : values.entrySet()) {
            // 写入字段名
            byte[] nameBytes = entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);

            // 写入字段值（支持 Integer 和 String）
            Object value = entry.getValue();
            if (value == null) {
                buffer.put((byte) 0);  // NULL
            } else if (value instanceof Integer) {
                buffer.put((byte) 1);  // Integer
                buffer.putInt((Integer) value);
            } else if (value instanceof String) {
                buffer.put((byte) 2);  // String
                byte[] valueBytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            } else if (value instanceof Long) {
                buffer.put((byte) 3);  // Long
                buffer.putLong((Long) value);
            } else {
                buffer.put((byte) 0);  // 不支持的类型当作 NULL
            }
        }
    }

    /**
     * 从页面反序列化节点
     */
    private BPlusTreeNode deserializeNode(Page page) {
        page.getData().position(38);  // 跳过页面头部

        // 读取节点类型
        boolean isLeaf = page.getData().get() == 1;

        BPlusTreeNode node = new BPlusTreeNode(page.getPageId(), isLeaf, order);

        // 读取键值数量
        int keyCount = page.getData().getInt();

        // 读取键值
        for (int i = 0; i < keyCount; i++) {
            node.getKeys().add(page.getData().getInt());
        }

        // 读取值或子节点指针
        if (isLeaf) {
            // 叶子节点：反序列化 DataRecord
            for (int i = 0; i < keyCount; i++) {
                DataRecord record = deserializeDataRecord(page.getData());
                node.getRecords().add(record);
            }
            node.setNextLeafPageId(page.getData().getInt());
            node.setPrevLeafPageId(page.getData().getInt());
        } else {
            // 内部节点：读取子节点指针
            for (int i = 0; i <= keyCount; i++) {
                node.getChildren().add(page.getData().getInt());
            }
        }

        node.setParentPageId(page.getData().getInt());
        return node;
    }

    /**
     * 从 ByteBuffer 反序列化 DataRecord
     */
    private DataRecord deserializeDataRecord(java.nio.ByteBuffer buffer) {
        DataRecord record = new DataRecord();

        // 读取隐藏列
        record.setDbRowId(buffer.getLong());
        record.setDbTrxId(buffer.getLong());
        record.setDbRollPtr(buffer.getLong());
        record.setDeleted(buffer.get() == 1);

        // 读取用户数据
        int fieldCount = buffer.getInt();

        for (int i = 0; i < fieldCount; i++) {
            // 读取字段名
            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String fieldName = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);

            // 读取字段值
            byte typeMarker = buffer.get();
            Object value;

            switch (typeMarker) {
                case 1:  // Integer
                    value = buffer.getInt();
                    break;
                case 2:  // String
                    int valueLength = buffer.getInt();
                    byte[] valueBytes = new byte[valueLength];
                    buffer.get(valueBytes);
                    value = new String(valueBytes, java.nio.charset.StandardCharsets.UTF_8);
                    break;
                case 3:  // Long
                    value = buffer.getLong();
                    break;
                default:  // NULL
                    value = null;
            }

            record.setValue(fieldName, value);
        }

        return record;
    }

    public int getRootPageId() {
        treeLock.readLock().lock();
        try {
            return rootPageId;
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * 更新完整记录
     *
     * @param key 主键
     * @param newRecord 新的 DataRecord
     * @return 是否更新成功
     */
    public boolean updateRecord(int key, DataRecord newRecord) throws IOException {
        treeLock.writeLock().lock();
        try {
            return updateRecordInNode(rootPageId, key, newRecord);
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    /**
     * 在节点中更新记录
     */
    private boolean updateRecordInNode(int pageId, int key, DataRecord newRecord) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        try {
            if (node.isLeaf()) {
                int pos = node.findKeyPosition(key);
                if (pos < node.getKeys().size() && node.getKeys().get(pos) == key) {
                    // 更新 DataRecord
                    node.getRecords().set(pos, newRecord);
                    serializeNode(node, page);
                    return true;
                }
                return false;
            } else {
                int childPageId = node.getChildren().get(findChildIndex(node, key));
                return updateRecordInNode(childPageId, key, newRecord);
            }
        } finally {
            bufferPool.unpinPage(pageId, page.isDirty());
        }
    }

    /**
     * 删除键值对（标记删除，不做物理删除）
     *
     * @param key 要删除的键
     * @return 是否删除成功
     */
    public boolean delete(int key) throws IOException {
        treeLock.writeLock().lock();
        try {
            return deleteInNode(rootPageId, key);
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private boolean deleteInNode(int pageId, int key) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        try {
            if (node.isLeaf()) {
                int pos = node.findKeyPosition(key);
                // 叶子节点，检查键是否存在
                if (pos < node.getKeys().size() && node.getKeys().get(pos) == key) {
                    // 删除键值对
                    node.getKeys().remove(pos);
                    node.getRecords().remove(pos);
                    serializeNode(node, page);
                    return true;
                }
                return false;
            } else {
                // 内部节点，递归查找
                int childPageId = node.getChildren().get(findChildIndex(node, key));
                return deleteInNode(childPageId, key);
            }
        } finally {
            bufferPool.unpinPage(pageId, page.isDirty());
        }
    }

    /**
     * 查找最左叶子节点（用于全表扫描）
     */
    public int findLeftmostLeaf() throws IOException {
        treeLock.readLock().lock();
        try {
            return findLeftmostLeafInNode(rootPageId);
        } finally {
            treeLock.readLock().unlock();
        }
    }

    private int findLeftmostLeafInNode(int pageId) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        try {
            if (node.isLeaf()) {
                return pageId;
            } else {
                // 获取最左子节点
                int childPageId = node.getChildren().get(0);
                return findLeftmostLeafInNode(childPageId);
            }
        } finally {
            bufferPool.unpinPage(pageId, false);
        }
    }

    /**
     * 获取叶子节点（用于迭代器）
     */
    public BPlusTreeNode getLeafNode(int pageId) throws IOException {
        treeLock.readLock().lock();
        try {
            Page page = bufferPool.fetchPage(pageId);
            return deserializeNode(page);
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * 释放叶子节点（用于迭代器）
     */
    public void releaseLeafNode(int pageId) throws IOException {
        treeLock.readLock().lock();
        try {
            bufferPool.unpinPage(pageId, false);
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * 插入递归返回结果：是否发生分裂，以及分裂后需要上推到父节点的分隔键与右子页。
     */
    private static final class InsertResult {
        private static final InsertResult NO_SPLIT = new InsertResult(false, 0, 0);
        private final boolean split;
        private final int separatorKey;
        private final int rightPageId;

        private InsertResult(boolean split, int separatorKey, int rightPageId) {
            this.split = split;
            this.separatorKey = separatorKey;
            this.rightPageId = rightPageId;
        }

        private static InsertResult split(int separatorKey, int rightPageId) {
            return new InsertResult(true, separatorKey, rightPageId);
        }

        private boolean hasSplit() {
            return split;
        }
    }

    private InsertResult insertIntoNode(int pageId, int key, DataRecord record) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        BPlusTreeNode node = deserializeNode(page);
        boolean dirty = false;
        try {
            if (node.isLeaf()) {
                int insertPos = node.findKeyPosition(key);
                node.getKeys().add(insertPos, key);
                node.getRecords().add(insertPos, record);
                dirty = true;

                if (node.getKeys().size() <= order) {
                    serializeNode(node, page);
                    return InsertResult.NO_SPLIT;
                }
                return splitLeafNode(node, page);
            }

            int childIndex = findChildIndex(node, key);
            int childPageId = node.getChildren().get(childIndex);
            InsertResult childResult = insertIntoNode(childPageId, key, record);
            if (!childResult.hasSplit()) {
                return InsertResult.NO_SPLIT;
            }

            node.getKeys().add(childIndex, childResult.separatorKey);
            node.getChildren().add(childIndex + 1, childResult.rightPageId);
            updateChildParent(childResult.rightPageId, node.getPageId());
            dirty = true;

            if (node.getKeys().size() <= order) {
                serializeNode(node, page);
                return InsertResult.NO_SPLIT;
            }
            return splitInternalNode(node, page);
        } finally {
            bufferPool.unpinPage(pageId, dirty || page.isDirty());
        }
    }

    private InsertResult splitLeafNode(BPlusTreeNode node, Page page) throws IOException {
        Page rightPage = bufferPool.newPage();
        int rightPageId = rightPage.getPageId();
        rightPage.setPageType(Page.PageType.INDEX_PAGE);

        BPlusTreeNode right = new BPlusTreeNode(rightPageId, true, order);
        int splitPos = node.getKeys().size() / 2;

        right.getKeys().addAll(new ArrayList<>(node.getKeys().subList(splitPos, node.getKeys().size())));
        right.getRecords().addAll(new ArrayList<>(node.getRecords().subList(splitPos, node.getRecords().size())));
        node.getKeys().subList(splitPos, node.getKeys().size()).clear();
        node.getRecords().subList(splitPos, node.getRecords().size()).clear();

        right.setParentPageId(node.getParentPageId());
        right.setPrevLeafPageId(node.getPageId());
        right.setNextLeafPageId(node.getNextLeafPageId());
        node.setNextLeafPageId(rightPageId);

        // 修复原 next 叶子节点的 prev 指针
        if (right.getNextLeafPageId() != -1) {
            Page nextPage = bufferPool.fetchPage(right.getNextLeafPageId());
            BPlusTreeNode next = deserializeNode(nextPage);
            next.setPrevLeafPageId(rightPageId);
            serializeNode(next, nextPage);
            bufferPool.unpinPage(nextPage.getPageId(), true);
        }

        serializeNode(node, page);
        serializeNode(right, rightPage);
        bufferPool.unpinPage(rightPageId, true);

        return InsertResult.split(right.getKeys().get(0), rightPageId);
    }

    private InsertResult splitInternalNode(BPlusTreeNode node, Page page) throws IOException {
        Page rightPage = bufferPool.newPage();
        int rightPageId = rightPage.getPageId();
        rightPage.setPageType(Page.PageType.INDEX_PAGE);

        int splitPos = node.getKeys().size() / 2;
        int separatorKey = node.getKeys().get(splitPos);

        BPlusTreeNode right = new BPlusTreeNode(rightPageId, false, order);
        right.setParentPageId(node.getParentPageId());

        right.getKeys().addAll(new ArrayList<>(node.getKeys().subList(splitPos + 1, node.getKeys().size())));
        right.getChildren().addAll(new ArrayList<>(node.getChildren().subList(splitPos + 1, node.getChildren().size())));

        node.getKeys().subList(splitPos, node.getKeys().size()).clear();
        node.getChildren().subList(splitPos + 1, node.getChildren().size()).clear();

        for (Integer childPageId : right.getChildren()) {
            updateChildParent(childPageId, rightPageId);
        }

        serializeNode(node, page);
        serializeNode(right, rightPage);
        bufferPool.unpinPage(rightPageId, true);

        return InsertResult.split(separatorKey, rightPageId);
    }

    private void createNewRoot(InsertResult result) throws IOException {
        int oldRootPageId = rootPageId;
        Page rootPage = bufferPool.newPage();
        int newRootPageId = rootPage.getPageId();
        rootPage.setPageType(Page.PageType.INDEX_PAGE);

        BPlusTreeNode newRoot = new BPlusTreeNode(newRootPageId, false, order);
        newRoot.getKeys().add(result.separatorKey);
        newRoot.getChildren().add(oldRootPageId);
        newRoot.getChildren().add(result.rightPageId);

        updateChildParent(oldRootPageId, newRootPageId);
        updateChildParent(result.rightPageId, newRootPageId);

        serializeNode(newRoot, rootPage);
        bufferPool.unpinPage(newRootPageId, true);
        rootPageId = newRootPageId;
    }

    /**
     * 内部节点 child 路由：等值走右子树（upper_bound）。
     */
    private int findChildIndex(BPlusTreeNode internalNode, int key) {
        List<Integer> keys = internalNode.getKeys();
        int left = 0;
        int right = keys.size();
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (key < keys.get(mid)) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private void updateChildParent(int childPageId, int parentPageId) throws IOException {
        Page childPage = bufferPool.fetchPage(childPageId);
        BPlusTreeNode childNode = deserializeNode(childPage);
        childNode.setParentPageId(parentPageId);
        serializeNode(childNode, childPage);
        bufferPool.unpinPage(childPageId, true);
    }
}
