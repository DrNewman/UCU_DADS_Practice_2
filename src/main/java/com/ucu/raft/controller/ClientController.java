package com.ucu.raft.controller;

import com.ucu.raft.config.RaftProperties;
import com.ucu.raft.service.RaftService;
import com.ucu.raft.service.RaftState;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class ClientController {

    private final RaftProperties raftProperties;
    private final RaftState raftState;
    private final RaftService raftService;

    @GetMapping("/state")
    public ResponseEntity<Map<String, Object>> getState() {
        return ResponseEntity.ok(Map.of(
                "nodeId", raftProperties.getNodeId(),
                "state", raftState.getRole().name(),
                "currentTerm", raftState.getCurrentTerm(),
                "commitIndex", raftState.getCommitIndex(),
                "lastApplied", raftState.getLastApplied(),
                "log", raftState.getLog()
        ));
    }

    @PostMapping("/command")
    public ResponseEntity<Map<String, String>> appendCommand(@RequestBody String command) {
        if (raftState.getRole() != com.ucu.raft.model.NodeRole.LEADER) {
            return ResponseEntity.status(400).body(Map.of(
                    "status", "NotALeader",
                    "nodeId", raftProperties.getNodeId()
            ));
        }

        boolean success = raftService.processClientCommand(command);
        if (success) {
            return ResponseEntity.ok(Map.of(
                    "status", "COMMITTED",
                    "nodeId", raftProperties.getNodeId(),
                    "command", command
            ));
        } else {
            return ResponseEntity.status(500).body(Map.of(
                    "status", "FAILED_TO_COMMIT",
                    "nodeId", raftProperties.getNodeId()
            ));
        }
    }
}