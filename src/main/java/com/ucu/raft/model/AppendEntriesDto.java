package com.ucu.raft.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

public class AppendEntriesDto {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Request {
        private long term;
        private String leaderId;
        private long prevLogIndex;
        private long prevLogTerm;
        private List<LogEntry> entries;
        private long leaderCommit;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Response {
        private long term;
        private boolean success;
        private long matchIndex;
    }
}