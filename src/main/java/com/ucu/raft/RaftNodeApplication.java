package com.ucu.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RaftNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(RaftNodeApplication.class, args);
    }
}