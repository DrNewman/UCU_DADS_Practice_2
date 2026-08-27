package com.ucu.raft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "raft")
public class RaftProperties {
    private String nodeId;
    private List<String> peers;

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setPeers(List<String> peers) {
        this.peers = peers;
    }
}