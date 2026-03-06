package com.minidb.lock;

import com.minidb.transaction.Transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 锁管理器（简化版）
 *
 * 设计目标：
 * 1. 当前读只依赖三类行级锁：记录锁、间隙锁、临键锁
 * 2. 冲突时采用“等待 + 超时返回 false”的最小并发语义
 * 3. 使用“主键区间重叠”做冲突判定，简化兼容MySQL但不完全等价
 */
public class LockManager {
    private static final long LOCK_WAIT_TIMEOUT_MS = 1000L;

    private final AtomicLong nextLockId;
    private final ConcurrentHashMap<String, List<Lock>> resourceLocks;
    private final ConcurrentHashMap<Integer, Set<String>> tableResources;
    private final ConcurrentHashMap<Long, Set<Lock>> transactionLocks;
    private final ReentrantLock managerLock;
    private final Condition lockReleased;
    private final AtomicLong totalLocksGranted;
    private final AtomicLong totalLocksReleased;
    private final AtomicLong totalLockWaits;

    public LockManager() {
        this.nextLockId = new AtomicLong(1);
        this.resourceLocks = new ConcurrentHashMap<>();
        this.tableResources = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.managerLock = new ReentrantLock();
        this.lockReleased = managerLock.newCondition();
        this.totalLocksGranted = new AtomicLong(0);
        this.totalLocksReleased = new AtomicLong(0);
        this.totalLockWaits = new AtomicLong(0);
    }

    public boolean acquireRowLock(Transaction txn, int tableId, String rowKey, LockMode mode) {
        String resourceId = "row:" + tableId + ":" + rowKey;
        return acquireLock(txn, mode, LockType.RECORD, resourceId, null, null);
    }

    public boolean acquireGapLock(Transaction txn, int tableId, int gapStart, int gapEnd, LockMode mode) {
        if (gapStart >= gapEnd) {
            return true;
        }
        String resourceId = "gap:" + tableId + ":" + gapStart + "-" + gapEnd;
        return acquireLock(txn, mode, LockType.GAP, resourceId, gapStart, gapEnd);
    }

    public boolean acquireNextKeyLock(Transaction txn, int tableId, String rowKey, int gapStart, int gapEnd, LockMode mode) {
        String resourceId = "nextkey:" + tableId + ":" + rowKey;
        return acquireLock(txn, mode, LockType.NEXT_KEY, resourceId, gapStart, gapEnd);
    }

    private boolean acquireLock(Transaction txn, LockMode mode,
                                LockType lockType, String resourceId, Integer gapStart, Integer gapEnd) {
        if (txn == null || !txn.isActive()) {
            return false;
        }
        if (mode != LockMode.X) {
            return false;
        }

        managerLock.lock();
        try {
            long transactionId = txn.getTransactionId();
            Lock request = new Lock(
                nextLockId.getAndIncrement(),
                lockType,
                resourceId,
                gapStart,
                gapEnd
            );
            long deadlineNanos = System.nanoTime() + LOCK_WAIT_TIMEOUT_MS * 1_000_000L;

            while (true) {
                if (alreadyHeld(resourceId, transactionId)) {
                    return true;
                }

                if (!hasConflict(transactionId, request)) {
                    request.addHolder(transactionId);
                    resourceLocks.computeIfAbsent(resourceId, k -> new ArrayList<>()).add(request);
                    Integer tableId = extractTableId(resourceId);
                    if (tableId != null) {
                        tableResources.computeIfAbsent(tableId, k -> new HashSet<>()).add(resourceId);
                    }
                    transactionLocks.computeIfAbsent(transactionId, k -> ConcurrentHashMap.newKeySet()).add(request);
                    totalLocksGranted.incrementAndGet();
                    return true;
                }

                totalLockWaits.incrementAndGet();
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    return false;
                }
                try {
                    lockReleased.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

        } finally {
            managerLock.unlock();
        }
    }

    public void releaseAllLocks(Transaction txn) {
        if (txn == null) {
            return;
        }

        managerLock.lock();
        try {
            long transactionId = txn.getTransactionId();
            Set<Lock> locks = transactionLocks.remove(transactionId);
            if (locks == null) {
                return;
            }

            for (Lock lock : locks) {
                releaseLock(lock, transactionId);
            }
            lockReleased.signalAll();

        } finally {
            managerLock.unlock();
        }
    }

    private void releaseLock(Lock lock, long transactionId) {
        lock.removeHolder(transactionId);

        if (!lock.hasHolders()) {
            List<Lock> locks = resourceLocks.get(lock.getResourceId());
            if (locks != null) {
                locks.remove(lock);
                if (locks.isEmpty()) {
                    resourceLocks.remove(lock.getResourceId());
                    Integer tableId = extractTableId(lock.getResourceId());
                    if (tableId != null) {
                        Set<String> resourceIds = tableResources.get(tableId);
                        if (resourceIds != null) {
                            resourceIds.remove(lock.getResourceId());
                            if (resourceIds.isEmpty()) {
                                tableResources.remove(tableId);
                            }
                        }
                    }
                }
            }
        }

        totalLocksReleased.incrementAndGet();
        lockReleased.signalAll();
    }

    public Set<Lock> getTransactionLocks(long transactionId) {
        Set<Lock> locks = transactionLocks.get(transactionId);
        return locks != null ? new HashSet<>(locks) : new HashSet<>();
    }

    public List<Lock> getResourceLocks(String resourceId) {
        List<Lock> locks = resourceLocks.get(resourceId);
        return locks != null ? new ArrayList<>(locks) : new ArrayList<>();
    }

    public String getStats() {
        return String.format(
                "LockManager Stats:\n" +
                "  Total Locks Granted: %d\n" +
                "  Total Locks Released: %d\n" +
                "  Total Lock Waits: %d\n" +
                "  Active Locks: %d\n" +
                "  Active Transactions with Locks: %d",
                totalLocksGranted.get(),
                totalLocksReleased.get(),
                totalLockWaits.get(),
                resourceLocks.size(),
                transactionLocks.size()
        );
    }

    private boolean alreadyHeld(String resourceId, long transactionId) {
        List<Lock> locks = resourceLocks.get(resourceId);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.isHeldBy(transactionId)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasConflict(long requestTxnId, Lock request) {
        Integer requestTableId = extractTableId(request.getResourceId());
        if (requestTableId == null) {
            return false;
        }

        Set<String> resources = tableResources.get(requestTableId);
        if (resources == null || resources.isEmpty()) {
            return false;
        }

        for (String resourceId : resources) {
            List<Lock> locks = resourceLocks.get(resourceId);
            if (locks == null) {
                continue;
            }
            for (Lock existing : locks) {
                if (conflictsOnResource(existing, request, requestTxnId)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean conflictsOnResource(Lock existing, Lock request, long requestTxnId) {
        if (existing.isHeldBy(requestTxnId)) {
            return false;
        }

        Integer existingTableId = extractTableId(existing.getResourceId());
        Integer requestTableId = extractTableId(request.getResourceId());
        if (existingTableId != null && requestTableId != null && !existingTableId.equals(requestTableId)) {
            return false;
        }

        KeyRange existingRange = toKeyRange(existing);
        KeyRange requestRange = toKeyRange(request);
        if (existingRange == null || requestRange == null) {
            return existing.getResourceId().equals(request.getResourceId());
        }

        return isOverlapping(existingRange, requestRange);
    }

    private Integer extractTableId(String resourceId) {
        if (resourceId == null) {
            return null;
        }
        String[] parts = resourceId.split(":");
        if (parts.length < 2) {
            return null;
        }
        try {
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer extractRowKey(String resourceId) {
        if (resourceId == null) {
            return null;
        }
        int lastColon = resourceId.lastIndexOf(':');
        if (lastColon < 0 || lastColon + 1 >= resourceId.length()) {
            return null;
        }
        try {
            return Integer.parseInt(resourceId.substring(lastColon + 1));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private KeyRange toKeyRange(Lock lock) {
        switch (lock.getLockType()) {
            case RECORD: {
                Integer key = extractRowKey(lock.getResourceId());
                if (key == null) {
                    return null;
                }
                return new KeyRange(key, key, true, true);
            }
            case GAP:
                if (lock.getGapStart() == null || lock.getGapEnd() == null) {
                    return null;
                }
                return new KeyRange(lock.getGapStart(), lock.getGapEnd(), false, false);
            case NEXT_KEY:
                if (lock.getGapStart() == null || lock.getGapEnd() == null) {
                    return null;
                }
                return new KeyRange(lock.getGapStart(), lock.getGapEnd(), false, true);
            default:
                return null;
        }
    }

    private boolean isOverlapping(KeyRange a, KeyRange b) {
        if (a.end < b.start) {
            return false;
        }
        if (a.end == b.start && !(a.includeEnd && b.includeStart)) {
            return false;
        }
        if (b.end < a.start) {
            return false;
        }
        if (b.end == a.start && !(b.includeEnd && a.includeStart)) {
            return false;
        }
        return true;
    }

    private static final class KeyRange {
        private final long start;
        private final long end;
        private final boolean includeStart;
        private final boolean includeEnd;

        private KeyRange(long start, long end, boolean includeStart, boolean includeEnd) {
            this.start = start;
            this.end = end;
            this.includeStart = includeStart;
            this.includeEnd = includeEnd;
        }
    }
}
