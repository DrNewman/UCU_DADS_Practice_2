package com.ucu.raft.model;

public class RequestVoteDto {

    public static class Request {
        private long term;
        private String candidateId;
        private long lastLogIndex;
        private long lastLogTerm;

        public Request() {}

        public Request(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        public long getTerm() {
            return term;
        }

        public String getCandidateId() {
            return candidateId;
        }

        public long getLastLogIndex() {
            return lastLogIndex;
        }

        public long getLastLogTerm() {
            return lastLogTerm;
        }
    }

    public static class Response {
        private long term;
        private boolean voteGranted;

        public Response() {}

        public Response(long term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }

        public long getTerm() {
            return term;
        }

        public boolean isVoteGranted() {
            return voteGranted;
        }
    }
}