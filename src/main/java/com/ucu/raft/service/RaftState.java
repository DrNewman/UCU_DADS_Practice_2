package com.ucu.raft.service;

import com.ucu.raft.model.LogEntry;
import com.ucu.raft.model.NodeRole;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@Getter
public class RaftState {

    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;

    private final List<LogEntry> log = Collections.synchronizedList(new ArrayList<>());

    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private final ConcurrentMap<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> matchIndex = new ConcurrentHashMap<>();

    public synchronized void setRole(NodeRole newRole) {
        this.role = newRole;
        if (newRole != NodeRole.CANDIDATE) {
            this.votedFor = null;
        }
    }

    public synchronized void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    public synchronized void setVotedFor(String candidateId) {
        this.votedFor = candidateId;
    }

    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public synchronized void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public synchronized void stepDown(long newTerm) {
        this.currentTerm = newTerm;
        this.role = NodeRole.FOLLOWER;
        this.votedFor = null;
    }

    public long getLastLogIndex() {
        synchronized (log) {
            return log.isEmpty() ? 0 : log.get(log.size() - 1).getIndex();
        }
    }

    public long getLastLogTerm() {
        synchronized (log) {
            return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
        }
    }
}