package com.minidb.storage;

import com.minidb.common.Config;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Buffer Pool - 缓冲池
 *
 * 职责：
 * 1. 管理页面缓存（LRU 替换）
 * 2. 跟踪脏页并在需要时刷盘
 * 3. 维护页面 pin/unpin 状态，避免使用中的页面被淘汰
 * 4. 通过读写锁保护 pageCache 的并发访问
 *
 * 关键字段：
 * - diskManager: 页面读写的持久化入口
 * - pageCache: PageId -> Page 的缓存映射（访问有序，用于 LRU）
 * - poolLock: 保护缓存结构修改的读写锁
 * - hitCount/missCount: 缓存命中统计
 */
public class BufferPool {
    private final DiskManager diskManager;
    private final int poolSize;

    // 页面缓存：PageId -> Page
    private final Map<Integer, Page> pageCache;

    // 全局锁（保护pageCache的结构性修改）
    private final ReentrantReadWriteLock poolLock;

    // 统计信息
    private long hitCount;
    private long missCount;

    public BufferPool(DiskManager diskManager) {
        this(diskManager, Config.BUFFER_POOL_SIZE);
    }

    public BufferPool(DiskManager diskManager, int poolSize) {
        this.diskManager = diskManager;
        this.poolSize = poolSize;
        this.poolLock = new ReentrantReadWriteLock();
        this.hitCount = 0;
        this.missCount = 0;

        // 使用LinkedHashMap实现LRU
        this.pageCache = new LinkedHashMap<Integer, Page>(poolSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
                if (size() > poolSize) {
                    Page page = eldest.getValue();
                    // 只有未被pin的页面才能被淘汰
                    if (!page.isPinned()) {
                        try {
                            evictPage(eldest.getKey(), page);
                            return true;
                        } catch (IOException e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                }
                return false;
            }
        };
    }

    /**
     * 获取页面（如果不在缓存中则从磁盘加载）
     */
    public Page fetchPage(int pageId) throws IOException {
        // 先尝试从缓存获取
        poolLock.readLock().lock();
        try {
            Page page = pageCache.get(pageId);
            if (page != null) {
                hitCount++;
                page.pin();
                return page;
            }
        } finally {
            poolLock.readLock().unlock();
        }

        // 缓存未命中，需要从磁盘加载
        missCount++;

        poolLock.writeLock().lock();
        try {
            // 双重检查，防止其他线程已经加载
            Page page = pageCache.get(pageId);
            if (page != null) {
                page.pin();
                return page;
            }

            // 创建新页面并从磁盘加载
            page = new Page(pageId);
            diskManager.readPage(pageId, page);
            page.pin();

            // 加入缓存
            pageCache.put(pageId, page);

            return page;
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * 创建新页面
     */
    public Page newPage() throws IOException {
        int pageId = diskManager.allocatePage();
        Page page = new Page(pageId);
        page.pin();

        poolLock.writeLock().lock();
        try {
            pageCache.put(pageId, page);
            return page;
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * Unpin页面
     */
    public void unpinPage(int pageId, boolean isDirty) {
        poolLock.readLock().lock();
        try {
            Page page = pageCache.get(pageId);
            if (page != null) {
                if (isDirty) {
                    page.markDirty();
                }
                page.unpin();
            }
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * 刷新指定页面到磁盘
     */
    public void flushPage(int pageId) throws IOException {
        poolLock.readLock().lock();
        try {
            Page page = pageCache.get(pageId);
            if (page != null && page.isDirty()) {
                diskManager.writePage(pageId, page);
                page.markClean();
            }
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * 刷新所有脏页到磁盘
     */
    public void flushAllPages() throws IOException {
        poolLock.readLock().lock();
        try {
            for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
                Page page = entry.getValue();
                if (page.isDirty()) {
                    diskManager.writePage(entry.getKey(), page);
                    page.markClean();
                }
            }
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * 淘汰页面
     */
    private void evictPage(int pageId, Page page) throws IOException {
        if (page.isDirty()) {
            diskManager.writePage(pageId, page);
        }
    }

    /**
     * 删除页面
     */
    public void deletePage(int pageId) throws IOException {
        poolLock.writeLock().lock();
        try {
            Page page = pageCache.remove(pageId);
            if (page != null) {
                if (page.isPinned()) {
                    throw new IllegalStateException("Cannot delete pinned page: " + pageId);
                }
                if (page.isDirty()) {
                    diskManager.writePage(pageId, page);
                }
            }
            diskManager.deallocatePage(pageId);
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * 获取缓存命中率
     */
    public double getHitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0 : (double) hitCount / total;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("BufferPool Stats: Size=%d/%d, HitRate=%.2f%%, Hits=%d, Misses=%d",
                pageCache.size(), poolSize, getHitRate() * 100, hitCount, missCount);
    }

    /**
     * 关闭缓冲池
     */
    public void shutdown() throws IOException {
        flushAllPages();
        pageCache.clear();
    }
}
