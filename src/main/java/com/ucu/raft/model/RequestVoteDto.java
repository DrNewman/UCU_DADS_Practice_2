package com.ucu.raft.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class RequestVoteDto {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Request {
        private long term;
        private String candidateId;
        private long lastLogIndex;
        private long lastLogTerm;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Response {
        private long term;
        private boolean voteGranted;
    }
}