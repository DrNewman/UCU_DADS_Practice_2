package com.ucu.raft.service;

import com.ucu.raft.config.RaftProperties;
import com.ucu.raft.model.AppendEntriesDto;
import com.ucu.raft.model.LogEntry;
import com.ucu.raft.model.NodeRole;
import com.ucu.raft.model.RequestVoteDto;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class RaftService {

    private final RaftState raftState;
    private final RaftProperties raftProperties;
    private final RaftClientSender raftClientSender;

    @Setter
    private RaftScheduler raftScheduler;

    // Запуск виборів
    public void startElection() {
        long termToRequest;
        String myId = raftProperties.getNodeId();
        List<String> peers = raftProperties.getPeers();

        synchronized (this) {
            if (raftState.getRole() == NodeRole.LEADER) {
                return;
            }

            raftState.setRole(NodeRole.CANDIDATE);
            raftState.setCurrentTerm(raftState.getCurrentTerm() + 1);
            raftState.setVotedFor(myId);
            termToRequest = raftState.getCurrentTerm();
            log.info("Node [{}] started election for Term {}", myId, termToRequest);
        }

        AtomicInteger votesGranted = new AtomicInteger(1); // Голос за себе
        int totalClusterSize = peers.size() + 1;
        int quorum = (totalClusterSize / 2) + 1;

        if (votesGranted.get() >= quorum) {
            becomeLeader();
            return;
        }

        RequestVoteDto.Request request;
        synchronized (this) {
            request = new RequestVoteDto.Request(
                    termToRequest,
                    myId,
                    raftState.getLastLogIndex(),
                    raftState.getLastLogTerm()
            );
        }

        for (String peerUrl : peers) {
            Thread.ofVirtual().start(() -> {
                RequestVoteDto.Response response = raftClientSender.sendRequestVote(peerUrl, request);
                if (response != null) {
                    synchronized (this) {
                        if (response.getTerm() > raftState.getCurrentTerm()) {
                            raftState.stepDown(response.getTerm());
                            if (raftScheduler != null) {
                                raftScheduler.resetElectionTimeout();
                            }
                            return;
                        }

                        if (raftState.getRole() == NodeRole.CANDIDATE && response.getTerm() == raftState.getCurrentTerm()) {
                            if (response.isVoteGranted()) {
                                int count = votesGranted.incrementAndGet();
                                log.info("Node [{}] received vote from {}. Total votes: {}/{}",
                                        myId, peerUrl, count, totalClusterSize);
                                if (count >= quorum) {
                                    becomeLeader();
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    private synchronized void becomeLeader() {
        if (raftState.getRole() != NodeRole.CANDIDATE) {
            return;
        }

        raftState.setRole(NodeRole.LEADER);
        log.info("🎉 Node [{}] became LEADER for Term {}!", raftProperties.getNodeId(), raftState.getCurrentTerm());

        long lastIndex = raftState.getLastLogIndex();
        for (String peer : raftProperties.getPeers()) {
            raftState.getNextIndex().put(peer, lastIndex + 1);
            raftState.getMatchIndex().put(peer, 0L);
        }

        sendHeartbeats();
    }

    // Розсилка Heartbeat / AppendEntries
    public void sendHeartbeats() {
        if (raftState.getRole() != NodeRole.LEADER) {
            return;
        }

        for (String peerUrl : raftProperties.getPeers()) {
            Thread.ofVirtual().start(() -> sendAppendEntriesToPeer(peerUrl));
        }
    }

    // Обробка команд клієнта
    public boolean processClientCommand(String command) {
        long newIndex;
        synchronized (this) {
            if (raftState.getRole() != NodeRole.LEADER) {
                return false;
            }

            newIndex = raftState.getLastLogIndex() + 1;
            long currentTerm = raftState.getCurrentTerm();
            LogEntry entry = new LogEntry(newIndex, currentTerm, command);

            raftState.getLog().add(entry);
            log.info("Node [{}] (LEADER) appended command at index {}: {}",
                    raftProperties.getNodeId(), newIndex, command);
        }

        return replicateLogToPeers(newIndex);
    }

    private boolean replicateLogToPeers(long targetIndex) {
        List<String> peers = raftProperties.getPeers();
        int totalClusterSize = peers.size() + 1;
        int quorum = (totalClusterSize / 2) + 1;

        AtomicInteger replicatedCount = new AtomicInteger(1);
        CompletableFuture<Boolean> quorumFuture = new CompletableFuture<>();

        for (String peerUrl : peers) {
            Thread.ofVirtual().start(() -> {
                boolean success = sendAppendEntriesToPeer(peerUrl);
                if (success) {
                    int count = replicatedCount.incrementAndGet();
                    if (count >= quorum) {
                        quorumFuture.complete(true);
                    }
                }
            });
        }

        try {
            Boolean reachedQuorum = quorumFuture.get(2000, TimeUnit.MILLISECONDS);
            if (Boolean.TRUE.equals(reachedQuorum)) {
                synchronized (this) {
                    if (targetIndex > raftState.getCommitIndex() && raftState.getRole() == NodeRole.LEADER) {
                        raftState.setCommitIndex(targetIndex);
                        raftState.setLastApplied(targetIndex);
                        log.info("Node [{}] COMMITTED index {}", raftProperties.getNodeId(), targetIndex);
                    }
                }
                return true;
            }
        } catch (Exception e) {
            log.warn("Node [{}] failed to reach quorum for index {}: {}",
                    raftProperties.getNodeId(), targetIndex, e.getMessage());
        }
        return false;
    }

    private boolean sendAppendEntriesToPeer(String peerUrl) {
        long prevLogIndex;
        long prevLogTerm;
        List<LogEntry> entriesToSend;
        long currentTerm;
        long commitIndex;

        synchronized (this) {
            if (raftState.getRole() != NodeRole.LEADER) {
                return false;
            }

            long nextIndexForPeer = raftState.getNextIndex().getOrDefault(peerUrl, 1L);
            prevLogIndex = nextIndexForPeer - 1;
            prevLogTerm = getLogTermAtIndex(prevLogIndex);
            entriesToSend = getEntriesFromIndex(nextIndexForPeer);

            currentTerm = raftState.getCurrentTerm();
            commitIndex = raftState.getCommitIndex();
        }

        AppendEntriesDto.Request request = new AppendEntriesDto.Request(
                currentTerm,
                raftProperties.getNodeId(),
                prevLogIndex,
                prevLogTerm,
                entriesToSend,
                commitIndex
        );

        AppendEntriesDto.Response response = raftClientSender.sendAppendEntries(peerUrl, request);

        if (response != null) {
            synchronized (this) {
                if (response.getTerm() > raftState.getCurrentTerm()) {
                    raftState.stepDown(response.getTerm());
                    return false;
                }

                if (raftState.getRole() == NodeRole.LEADER && response.getTerm() == raftState.getCurrentTerm()) {
                    if (response.isSuccess()) {
                        long lastSentIndex = prevLogIndex + entriesToSend.size();
                        raftState.getNextIndex().put(peerUrl, lastSentIndex + 1);
                        raftState.getMatchIndex().put(peerUrl, lastSentIndex);
                        return true;
                    } else {
                        // Якщо відмова -> знижуємо nextIndex, щоб підібрати спільну точку логу (Крок 7)
                        long currentNext = raftState.getNextIndex().getOrDefault(peerUrl, 1L);
                        if (currentNext > 1) {
                            raftState.getNextIndex().put(peerUrl, currentNext - 1);
                        }
                    }
                }
            }
        }
        return false;
    }

    // Обробка RequestVote
    public synchronized RequestVoteDto.Response handleRequestVote(RequestVoteDto.Request request) {
        if (request.getTerm() < raftState.getCurrentTerm()) {
            return new RequestVoteDto.Response(raftState.getCurrentTerm(), false);
        }

        if (request.getTerm() > raftState.getCurrentTerm()) {
            raftState.stepDown(request.getTerm());
        }

        boolean canVote = (raftState.getVotedFor() == null || raftState.getVotedFor().equals(request.getCandidateId()));
        boolean logIsUpToDate = isLogUpToDate(request.getLastLogTerm(), request.getLastLogIndex());

        if (canVote && logIsUpToDate) {
            raftState.setVotedFor(request.getCandidateId());
            if (raftScheduler != null) {
                raftScheduler.resetElectionTimeout();
            }
            log.info("Node [{}] voted FOR candidate [{}] in Term {}",
                    raftProperties.getNodeId(), request.getCandidateId(), request.getTerm());
            return new RequestVoteDto.Response(raftState.getCurrentTerm(), true);
        }

        return new RequestVoteDto.Response(raftState.getCurrentTerm(), false);
    }

    // Обробка AppendEntries
    public synchronized AppendEntriesDto.Response handleAppendEntries(AppendEntriesDto.Request request) {
        if (request.getTerm() < raftState.getCurrentTerm()) {
            return new AppendEntriesDto.Response(raftState.getCurrentTerm(), false, 0);
        }

        if (request.getTerm() > raftState.getCurrentTerm() || raftState.getRole() == NodeRole.CANDIDATE) {
            raftState.stepDown(request.getTerm());
        }

        if (raftScheduler != null) {
            raftScheduler.resetElectionTimeout();
        }

        if (request.getPrevLogIndex() > 0) {
            if (request.getPrevLogIndex() > raftState.getLastLogIndex()) {
                return new AppendEntriesDto.Response(raftState.getCurrentTerm(), false, raftState.getLastLogIndex());
            }
            long termAtPrev = getLogTermAtIndex(request.getPrevLogIndex());
            if (termAtPrev != request.getPrevLogTerm()) {
                truncateLogFrom(request.getPrevLogIndex());
                return new AppendEntriesDto.Response(raftState.getCurrentTerm(), false, raftState.getLastLogIndex());
            }
        }

        if (request.getEntries() != null && !request.getEntries().isEmpty()) {
            for (LogEntry newEntry : request.getEntries()) {
                long index = newEntry.getIndex();
                if (index <= raftState.getLastLogIndex()) {
                    if (getLogTermAtIndex(index) != newEntry.getTerm()) {
                        truncateLogFrom(index);
                        raftState.getLog().add(newEntry);
                    }
                } else {
                    raftState.getLog().add(newEntry);
                }
            }
        }

        if (request.getLeaderCommit() > raftState.getCommitIndex()) {
            long newCommitIndex = Math.min(request.getLeaderCommit(), raftState.getLastLogIndex());
            raftState.setCommitIndex(newCommitIndex);
            raftState.setLastApplied(newCommitIndex);
        }

        return new AppendEntriesDto.Response(raftState.getCurrentTerm(), true, raftState.getLastLogIndex());
    }

    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = raftState.getLastLogTerm();
        long myLastIndex = raftState.getLastLogIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    private long getLogTermAtIndex(long index) {
        if (index <= 0 || index > raftState.getLog().size()) {
            return 0;
        }
        return raftState.getLog().get((int) index - 1).getTerm();
    }

    private List<LogEntry> getEntriesFromIndex(long fromIndex) {
        List<LogEntry> result = new ArrayList<>();
        List<LogEntry> currentLog = raftState.getLog();
        int startIndex = (int) fromIndex - 1;
        if (startIndex >= 0 && startIndex < currentLog.size()) {
            for (int i = startIndex; i < currentLog.size(); i++) {
                result.add(currentLog.get(i));
            }
        }
        return result;
    }

    private void truncateLogFrom(long index) {
        int fromIndex = (int) index - 1;
        List<LogEntry> currentLog = raftState.getLog();
        while (currentLog.size() > fromIndex && !currentLog.isEmpty()) {
            currentLog.removeLast();
        }
    }
}