package com.ucu.raft.controller;

import com.ucu.raft.model.AppendEntriesDto;
import com.ucu.raft.model.RequestVoteDto;
import com.ucu.raft.service.RaftService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/raft")
public class RaftInternalController {

    private final RaftService raftService;

    public RaftInternalController(RaftService raftService) {
        this.raftService = raftService;
    }

    @PostMapping("/request-vote")
    public ResponseEntity<RequestVoteDto.Response> requestVote(@RequestBody RequestVoteDto.Request request) {
        return ResponseEntity.ok(raftService.handleRequestVote(request));
    }

    @PostMapping("/append-entries")
    public ResponseEntity<AppendEntriesDto.Response> appendEntries(@RequestBody AppendEntriesDto.Request request) {
        return ResponseEntity.ok(raftService.handleAppendEntries(request));
    }
}