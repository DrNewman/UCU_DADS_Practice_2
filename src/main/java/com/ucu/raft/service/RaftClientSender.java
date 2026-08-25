package com.ucu.raft.service;

import com.ucu.raft.model.AppendEntriesDto;
import com.ucu.raft.model.RequestVoteDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Slf4j
@Service
public class RaftClientSender {

    private final RestTemplate restTemplate;

    public RaftClientSender(RestTemplateBuilder builder) {
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofMillis(200))
                .setReadTimeout(Duration.ofMillis(200))
                .build();
    }

    public RequestVoteDto.Response sendRequestVote(String peerUrl, RequestVoteDto.Request request) {
        try {
            return restTemplate.postForObject(
                    peerUrl + "/internal/raft/request-vote",
                    request,
                    RequestVoteDto.Response.class
            );
        } catch (Exception e) {
            log.trace("Failed to send RequestVote to {}: {}", peerUrl, e.getMessage());
            return null;
        }
    }

    public AppendEntriesDto.Response sendAppendEntries(String peerUrl, AppendEntriesDto.Request request) {
        try {
            return restTemplate.postForObject(
                    peerUrl + "/internal/raft/append-entries",
                    request,
                    AppendEntriesDto.Response.class
            );
        } catch (Exception e) {
            log.trace("Failed to send AppendEntries to {}: {}", peerUrl, e.getMessage());
            return null;
        }
    }
}