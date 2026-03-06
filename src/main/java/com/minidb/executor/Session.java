package com.minidb.executor;

import com.minidb.transaction.Transaction;

/**
 * 会话对象。
 * 代表一个客户端连接/执行上下文，持有该会话当前正在运行的事务。
 */
public class Session {
    private final String sessionId;
    private volatile Transaction currentTransaction;

    public Session(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Transaction getCurrentTransaction() {
        return currentTransaction;
    }

    public void setCurrentTransaction(Transaction currentTransaction) {
        this.currentTransaction = currentTransaction;
    }
}
