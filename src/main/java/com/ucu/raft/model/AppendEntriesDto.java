package com.ucu.raft.model;

import java.util.List;

public class AppendEntriesDto {

    public static class Request {
        private long term;
        private String leaderId;
        private long prevLogIndex;
        private long prevLogTerm;
        private List<LogEntry> entries;
        private long leaderCommit;

        public Request() {}

        public Request(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }

        public long getTerm() {
            return term;
        }

        public long getPrevLogIndex() {
            return prevLogIndex;
        }

        public long getPrevLogTerm() {
            return prevLogTerm;
        }

        public List<LogEntry> getEntries() {
            return entries;
        }

        public long getLeaderCommit() {
            return leaderCommit;
        }
    }

    public static class Response {
        private long term;
        private boolean success;
        private long matchIndex;

        public Response() {}

        public Response(long term, boolean success, int matchIndex) {
            this.term = term;
            this.success = success;
            this.matchIndex = matchIndex;
        }

        public Response(long term, boolean success, long matchIndex) {
            this.term = term;
            this.success = success;
            this.matchIndex = matchIndex;
        }

        public long getTerm() {
            return term;
        }

        public boolean isSuccess() {
            return success;
        }
    }
}