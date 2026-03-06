package com.minidb.index;

import com.minidb.common.DataRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * B+树节点
 * 内部节点：存储键值和子节点指针
 * 叶子节点：存储键值和数据记录指针（pageId）
 */
public class BPlusTreeNode {
    // 是否为叶子节点
    private boolean isLeaf;

    // 节点对应的页面ID
    private int pageId;

    // 父节点页面ID
    private int parentPageId;

    // 键值列表
    private List<Integer> keys;

    // 子节点页面ID列表（内部节点）
    private List<Integer> children;

    // 数据记录列表（叶子节点）- 改为存储完整 DataRecord
    private List<DataRecord> records;

    // 叶子节点的前后指针（用于范围查询）
    private int nextLeafPageId;
    private int prevLeafPageId;

    // 最大键值数量（B+树的阶）
    private final int maxKeys;

    public BPlusTreeNode(int pageId, boolean isLeaf, int maxKeys) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.maxKeys = maxKeys;
        this.parentPageId = -1;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.records = new ArrayList<>();
        this.nextLeafPageId = -1;
        this.prevLeafPageId = -1;
    }

    /**
     * 查找键值在节点中的位置
     */
    public int findKeyPosition(int key) {
        int left = 0, right = keys.size() - 1;
        int pos = keys.size();

        while (left <= right) {
            int mid = (left + right) / 2;
            if (keys.get(mid) < key) {
                left = mid + 1;
            } else {
                pos = mid;
                right = mid - 1;
            }
        }

        return pos;
    }

    // Getters and Setters
    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public int getParentPageId() {
        return parentPageId;
    }

    public void setParentPageId(int parentPageId) {
        this.parentPageId = parentPageId;
    }

    public List<Integer> getKeys() {
        return keys;
    }

    public void setKeys(List<Integer> keys) {
        this.keys = keys;
    }

    public List<Integer> getChildren() {
        return children;
    }

    public void setChildren(List<Integer> children) {
        this.children = children;
    }

    /**
     * 获取记录列表（叶子节点）
     */
    public List<DataRecord> getRecords() {
        return records;
    }

    public void setRecords(List<DataRecord> records) {
        this.records = records;
    }

    public int getNextLeafPageId() {
        return nextLeafPageId;
    }

    public void setNextLeafPageId(int nextLeafPageId) {
        this.nextLeafPageId = nextLeafPageId;
    }

    public int getPrevLeafPageId() {
        return prevLeafPageId;
    }

    public void setPrevLeafPageId(int prevLeafPageId) {
        this.prevLeafPageId = prevLeafPageId;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    @Override
    public String toString() {
        return String.format("BPlusTreeNode[pageId=%d, isLeaf=%b, keys=%s, keyCount=%d]",
                pageId, isLeaf, keys, keys.size());
    }
}
