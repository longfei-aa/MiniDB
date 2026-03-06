package com.minidb.storage;

import com.minidb.common.Config;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 页面类 - 数据库存储的基本单位
 * 页面大小：8KB
 *
 * 当前实现的页面布局（简化版）：
 * - Page Header (38 bytes)
 * - 记录数据区/空闲区（由 freeSpaceOffset 向后增长）
 * - 预留 File Trailer 空间 (8 bytes)
 */
public class Page {
    // 页面ID
    private final int pageId;

    // 页面数据
    private final ByteBuffer data;

    // 页面类型
    private PageType pageType;

    // 脏页标记
    private volatile boolean dirty;

    // 引用计数（Pin计数）
    private final AtomicInteger pinCount;

    public enum PageType {
        INDEX_PAGE(0x45BF),     // B+树索引页
        DATA_PAGE(0x45BD),      // 数据页
        UNDO_PAGE(0x45B0),      // Undo日志页
        FREE_PAGE(0x0000);      // 空闲页

        private final int typeCode;

        PageType(int typeCode) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }
    }

    public Page(int pageId) {
        this.pageId = pageId;
        this.data = ByteBuffer.allocate(Config.PAGE_SIZE);
        this.pageType = PageType.FREE_PAGE;
        this.dirty = false;
        this.pinCount = new AtomicInteger(0);
        initPageHeader();
    }

    /**
     * 初始化页面头部
     * Page Header结构（38 bytes）：
     * - Checksum (4 bytes)
     * - Page ID (4 bytes)
     * - LSN (8 bytes) - Log Sequence Number
     * - Page Type (2 bytes)
     * - Next Page (4 bytes)
     * - Prev Page (4 bytes)
     * - Free Space Offset (2 bytes)
     * - Record Count (2 bytes)
     * - Max Transaction ID (8 bytes)
     */
    private void initPageHeader() {
        data.position(0);
        data.putInt(0);              // Checksum
        data.putInt(pageId);         // Page ID
        data.putLong(0);             // LSN
        data.putShort((short) pageType.getTypeCode());  // Page Type
        data.putInt(-1);             // Next Page (-1表示无)
        data.putInt(-1);             // Prev Page (-1表示无)
        data.putShort((short) 38);   // Free Space Offset (紧跟Header)
        data.putShort((short) 0);    // Record Count
        data.putLong(0);             // Max Transaction ID
    }

    // Pin操作：增加引用计数
    public void pin() {
        pinCount.incrementAndGet();
    }

    // Unpin操作：减少引用计数
    public void unpin() {
        if (pinCount.get() > 0) {
            pinCount.decrementAndGet();
        }
    }

    public boolean isPinned() {
        return pinCount.get() > 0;
    }

    // Getter和Setter方法
    public int getPageId() {
        return pageId;
    }

    public ByteBuffer getData() {
        return data;
    }

    public PageType getPageType() {
        return pageType;
    }

    public void setPageType(PageType pageType) {
        this.pageType = pageType;
        // 更新页面头部的类型字段
        data.putShort(12, (short) pageType.getTypeCode());
        markDirty();
    }

    public boolean isDirty() {
        return dirty;
    }

    public void markDirty() {
        this.dirty = true;
    }

    public void markClean() {
        this.dirty = false;
    }

    public int getPinCount() {
        return pinCount.get();
    }

    /**
     * 获取空闲空间偏移量
     */
    public int getFreeSpaceOffset() {
        return data.getShort(30) & 0xFFFF;
    }

    /**
     * 设置空闲空间偏移量
     */
    public void setFreeSpaceOffset(int offset) {
        data.putShort(30, (short) offset);
        markDirty();
    }

    /**
     * 获取记录数量
     */
    public int getRecordCount() {
        return data.getShort(32) & 0xFFFF;
    }

    /**
     * 设置记录数量
     */
    public void setRecordCount(int count) {
        data.putShort(32, (short) count);
        markDirty();
    }

    /**
     * 获取可用空闲空间大小
     */
    public int getFreeSpace() {
        // 末尾预留 8 字节 trailer 空间（当前未写入具体 trailer 内容）
        return Config.PAGE_SIZE - getFreeSpaceOffset() - 8;
    }

    @Override
    public String toString() {
        return String.format("Page[id=%d, type=%s, dirty=%b, pinCount=%d, freeSpace=%d]",
                pageId, pageType, dirty, pinCount.get(), getFreeSpace());
    }
}
