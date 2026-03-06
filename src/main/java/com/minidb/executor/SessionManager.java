package com.minidb.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话管理器。
 * 负责按 sessionId 创建和复用 Session，供 Executor 维护多会话事务上下文。
 */
public class SessionManager {
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    public Session getOrCreateSession(String sessionId) {
        return sessions.computeIfAbsent(sessionId, Session::new);
    }

    public Session getSession(String sessionId) {
        return sessions.get(sessionId);
    }

    public List<Session> getAllSessions() {
        return new ArrayList<>(sessions.values());
    }

    public void removeSession(String sessionId) {
        sessions.remove(sessionId);
    }
}
