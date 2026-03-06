package com.minidb.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ReadView - MVCC快照读视图
 * 用于实现快照隔离，判断数据版本的可见性
 */
public class ReadView {
    // 创建ReadView时的所有活跃事务ID列表（已排序）
    private final List<Long> activeTransactionIds;

    // 最小活跃事务ID（min_trx_id）
    private final long minActiveTransactionId;

    // 创建快照时看到的“下一个待分配事务ID”（up_limit_id）
    private final long nextTransactionId;

    // 创建此ReadView的事务ID
    private final long creatorTransactionId;

    // ReadView创建时间（用于调试和监控）
    private final long createTime;

    /**
     * 构造函数
     *
     * @param activeTransactionIds 活跃事务ID列表
     * @param nextTransactionId 下一个待分配的事务ID
     * @param creatorTransactionId 创建此ReadView的事务ID
     */
    public ReadView(List<Long> activeTransactionIds, long nextTransactionId, long creatorTransactionId) {
        // 复制并排序活跃事务列表
        this.activeTransactionIds = new ArrayList<>(activeTransactionIds);
        Collections.sort(this.activeTransactionIds);

        // 计算最小活跃事务ID
        if (this.activeTransactionIds.isEmpty()) {
            this.minActiveTransactionId = nextTransactionId;
        } else {
            this.minActiveTransactionId = this.activeTransactionIds.get(0);
        }

        this.nextTransactionId = nextTransactionId;
        this.creatorTransactionId = creatorTransactionId;
        this.createTime = System.currentTimeMillis();
    }

    /**
     * 判断版本是否对当前ReadView可见
     *
     * 可见性判断规则（核心算法）：
     * 1. 如果数据版本的事务ID == creatorTransactionId，可见（自己的修改）
     * 2. 如果数据版本的事务ID < minActiveTransactionId，可见（已提交的旧事务）
     * 3. 如果数据版本的事务ID >= nextTransactionId，不可见（快照创建后才分配的“未来版本”）
     * 4. 如果数据版本的事务ID在活跃事务列表中，不可见（未提交）
     * 5. 否则可见（已提交的事务，且不在活跃列表中）
     *
     * @param versionTransactionId 数据版本的事务ID
     * @return true表示可见，false表示不可见
     */
    public boolean isVisible(long versionTransactionId) {
        // 规则1: 自己创建的数据，肯定可见
        if (versionTransactionId == creatorTransactionId) {
            return true;
        }

        // 规则2: 版本创建于最小活跃事务ID之前，说明该事务已提交，可见
        if (versionTransactionId < minActiveTransactionId) {
            return true;
        }

        // 规则3: 版本创建于ReadView生成之后，不可见
        if (versionTransactionId >= nextTransactionId) {
            return false;
        }

        // 规则4: 在活跃事务列表中，说明未提交，不可见
        // 使用二分查找优化性能
        if (Collections.binarySearch(activeTransactionIds, versionTransactionId) >= 0) {
            return false;
        }

        // 规则5: 不在活跃列表中，且ID在范围内，说明已提交，可见
        return true;
    }

    /**
     * 获取活跃事务ID列表
     */
    public List<Long> getActiveTransactionIds() {
        return new ArrayList<>(activeTransactionIds);
    }

    /**
     * 获取最小活跃事务ID
     */
    public long getMinActiveTransactionId() {
        return minActiveTransactionId;
    }

    /**
     * 获取最大事务ID
     */
    public long getNextTransactionId() {
        return nextTransactionId;
    }

    /**
     * 获取创建者事务ID
     */
    public long getCreatorTransactionId() {
        return creatorTransactionId;
    }

    /**
     * 获取创建时间
     */
    public long getCreateTime() {
        return createTime;
    }

    @Override
    public String toString() {
        return String.format(
                "ReadView[creator=%d, min=%d, max=%d, active=%s]",
                creatorTransactionId,
                minActiveTransactionId,
                nextTransactionId,
                activeTransactionIds
        );
    }
}
