package com.minidb.executor;

import com.minidb.common.DataRecord;
import com.minidb.index.BPlusTree;
import com.minidb.index.BPlusTreeNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 表迭代器 - 用于全表扫描
 * 遍历 B+ 树的所有叶子节点，返回所有记录的主键
 */
public class TableIterator implements Iterator<Integer> {
    private final BPlusTree index;
    private BPlusTreeNode currentLeaf;
    private int currentIndex;
    private int currentLeafPageId;

    public TableIterator(BPlusTree index) throws IOException {
        this.index = index;

        // 定位到最左叶子节点
        this.currentLeafPageId = index.findLeftmostLeaf();
        if (currentLeafPageId != -1) {
            this.currentLeaf = index.getLeafNode(currentLeafPageId);
            this.currentIndex = 0;
        } else {
            this.currentLeaf = null;
            this.currentIndex = 0;
        }
    }

    @Override
    public boolean hasNext() {
        if (currentLeaf == null) {
            return false;
        }

        // 如果当前叶子节点还有数据
        if (currentIndex < currentLeaf.getKeys().size()) {
            return true;
        }

        // 检查是否有下一个叶子节点
        int nextPageId = currentLeaf.getNextLeafPageId();
        return nextPageId != -1 && nextPageId != 0;  // 检查有效的下一个页面
    }

    @Override
    public Integer next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }

        try {
            // 如果当前叶子节点遍历完，移动到下一个叶子节点
            if (currentIndex >= currentLeaf.getKeys().size()) {
                // 释放当前叶子节点
                index.releaseLeafNode(currentLeafPageId);

                // 移动到下一个叶子节点
                int nextPageId = currentLeaf.getNextLeafPageId();
                if (nextPageId == -1 || nextPageId == 0) {
                    currentLeaf = null;
                    throw new NoSuchElementException("No more elements");
                }

                currentLeafPageId = nextPageId;
                currentLeaf = index.getLeafNode(currentLeafPageId);
                currentIndex = 0;
            }

            // 返回当前记录的主键值
            Integer key = currentLeaf.getKeys().get(currentIndex);
            currentIndex++;

            return key;

        } catch (IOException e) {
            throw new RuntimeException("Error reading leaf node", e);
        }
    }

    /**
     * 关闭迭代器，释放资源
     */
    public void close() {
        try {
            if (currentLeafPageId != -1) {
                index.releaseLeafNode(currentLeafPageId);
            }
        } catch (IOException e) {
            // 忽略关闭时的异常
        }
    }
}
