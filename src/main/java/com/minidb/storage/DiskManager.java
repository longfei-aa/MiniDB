package com.minidb.storage;

import com.minidb.common.Config;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.LinkedList;


/**
 * 磁盘管理器 - 负责页面的持久化存储
 * 功能：
 * 1. 页面的读取和写入
 * 2. 文件空间管理
 * 3. 分配新页面
 */
public class DiskManager {
    private final String dataFilePath;
    private RandomAccessFile dataFile;
    private FileChannel fileChannel;
    private final AtomicInteger nextPageId;

    // 空闲页链表（头部插入，提高空间局部性）
    private final LinkedList<Integer> freePageList;

    public DiskManager(String dbName) throws IOException {
        this.dataFilePath = Config.DATA_DIR + dbName + ".db";
        this.nextPageId = new AtomicInteger(0);
        this.freePageList = new LinkedList<>();
        initDataFile();
    }

    /**
     * 初始化数据文件
     */
    private void initDataFile() throws IOException {
        File dataDir = new File(Config.DATA_DIR);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        File file = new File(dataFilePath);
        boolean isNewFile = !file.exists();

        this.dataFile = new RandomAccessFile(file, "rw");
        this.fileChannel = dataFile.getChannel();

        if (isNewFile) {
            // 新文件，写入文件头
            writeFileHeader();
        } else {
            // 已存在的文件，读取文件头
            readFileHeader();
        }
    }

    /**
     * 写入文件头
     * 文件头结构（1页，8KB）：
     * - Magic Number (4 bytes): "MNDB"
     * - Version (4 bytes)
     * - Page Count (4 bytes)
     * - First Free Page (4 bytes)
     * - Reserved (剩余空间)
     */
    private void writeFileHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(Config.PAGE_SIZE);
        header.put("MNDB".getBytes());  // Magic Number
        header.putInt(1);                // Version
        header.putInt(1);                // Page Count (文件头本身占1页)
        header.putInt(-1);               // First Free Page (无空闲页)

        header.position(0);
        fileChannel.position(0);
        fileChannel.write(header);
        fileChannel.force(true);

        nextPageId.set(1);  // 第0页是文件头，从第1页开始分配
    }

    /**
     * 读取文件头
     */
    private void readFileHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(Config.PAGE_SIZE);
        fileChannel.position(0);
        fileChannel.read(header);
        header.flip();

        byte[] magic = new byte[4];
        header.get(magic);
        String magicStr = new String(magic);

        if (!"MNDB".equals(magicStr)) {
            throw new IOException("Invalid database file format");
        }

        int version = header.getInt();
        int pageCount = header.getInt();
        int firstFreePage = header.getInt();

        nextPageId.set(pageCount);
    }

    /**
     * 分配新页面
     * 优先从空闲页链表获取，如果没有则分配新页面
     */
    public synchronized int allocatePage() throws IOException {
        int pageId;

        // 优先从空闲页链表获取
        if (!freePageList.isEmpty()) {
            pageId = freePageList.removeFirst();
            System.out.println("Reusing free page: " + pageId);
        } else {
            // 分配新页面
            pageId = nextPageId.getAndIncrement();
            // 更新文件头的页面计数
            updatePageCount(pageId + 1);
        }

        return pageId;
    }

    /**
     * 更新文件头中的页面计数
     */
    private void updatePageCount(int count) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(count);
        buffer.flip();

        fileChannel.position(8);  // Magic(4) + Version(4)
        fileChannel.write(buffer);
    }

    /**
     * 读取页面
     */
    public void readPage(int pageId, Page page) throws IOException {
        if (pageId <= 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        long offset = (long) pageId * Config.PAGE_SIZE;
        ByteBuffer buffer = page.getData();
        buffer.clear();

        synchronized (fileChannel) {
            fileChannel.position(offset);
            int totalBytesRead = 0;
            while (buffer.hasRemaining()) {
                int bytesRead = fileChannel.read(buffer);
                if (bytesRead < 0) {
                    break;
                }
                if (bytesRead == 0) {
                    break;
                }
                totalBytesRead += bytesRead;
            }

            if (totalBytesRead < Config.PAGE_SIZE) {
                // 页面不存在或未完全写入，未读部分补零，避免读到旧脏字节
                while (buffer.position() < Config.PAGE_SIZE) {
                    buffer.put((byte) 0);
                }
                buffer.flip();
            } else {
                buffer.flip();
            }
        }

        page.markClean();
    }

    /**
     * 写入页面
     */
    public void writePage(int pageId, Page page) throws IOException {
        if (pageId <= 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        long offset = (long) pageId * Config.PAGE_SIZE;
        ByteBuffer buffer = page.getData();
        buffer.position(0);
        buffer.limit(Config.PAGE_SIZE);

        synchronized (fileChannel) {
            fileChannel.position(offset);
            while (buffer.hasRemaining()) {
                int bytesWritten = fileChannel.write(buffer);
                if (bytesWritten <= 0) {
                    throw new IOException("Failed to write full page, pageId=" + pageId);
                }
            }
            fileChannel.force(false);
        }

        page.markClean();
    }

    /**
     * 释放页面（标记为空闲）
     * 将页面加入空闲页链表以便重用
     */
    public synchronized void deallocatePage(int pageId) {
        if (pageId <= 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        // 加入空闲页链表（头部插入，提高空间局部性）
        if (!freePageList.contains(pageId)) {
            freePageList.addFirst(pageId);
            System.out.println("Page " + pageId + " added to free list");
        }
    }

    /**
     * 获取空闲页数量（用于监控）
     */
    public int getFreePageCount() {
        return freePageList.size();
    }

    /**
     * 刷新所有更改到磁盘
     */
    public void flush() throws IOException {
        fileChannel.force(true);
    }

    /**
     * 关闭磁盘管理器
     */
    public void close() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
        if (dataFile != null) {
            dataFile.close();
        }
    }

    public int getPageCount() {
        return nextPageId.get();
    }

    public String getDataFilePath() {
        return dataFilePath;
    }
}
