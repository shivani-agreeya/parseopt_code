package com.verizon.oneparser.hdfscopier.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class JobConfiguration {

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
