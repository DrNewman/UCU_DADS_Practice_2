package com.ucu.raft.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class RaftConfig {

    @Bean
    public RestClient raftRestClient() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(150);
        requestFactory.setReadTimeout(150);

        return RestClient.builder()
                .requestFactory(requestFactory)
                .build();
    }
}