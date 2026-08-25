package com.ucu.raft.service;

import com.ucu.raft.model.NodeRole;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class RaftScheduler {

    private final RaftService raftService;
    private final RaftState raftState;
    private final Random random = new Random();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private ScheduledFuture<?> electionTimeoutTask;

    @PostConstruct
    public void init() {
        raftService.setRaftScheduler(this);
        resetElectionTimeout();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (raftState.getRole() == NodeRole.LEADER) {
                    raftService.sendHeartbeats();
                }
            } catch (Exception e) {
                log.error("Error sending heartbeats", e);
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    public void resetElectionTimeout() {
        synchronized (this) {
            if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
                electionTimeoutTask.cancel(true);
            }

            int timeout = 1000 + random.nextInt(1000); // 1000 - 2000 ms

            electionTimeoutTask = scheduler.schedule(() -> {
                try {
                    if (raftState.getRole() != NodeRole.LEADER) {
                        log.info("Election timeout reached! Starting election...");
                        raftService.startElection();
                        resetElectionTimeout();
                    }
                } catch (Exception e) {
                    log.error("Error in election timeout task", e);
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}