package com.ucu.raft.model;

public class LogEntry {
    private long index;
    private long term;
    private String command;

    public LogEntry() {}

    public LogEntry(long index, long term, String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }
}