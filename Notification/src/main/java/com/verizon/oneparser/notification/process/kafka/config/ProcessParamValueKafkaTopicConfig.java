package com.verizon.oneparser.notification.process.kafka.config;

import lombok.RequiredArgsConstructor;
import com.verizon.oneparser.notification.properties.NotificationKafkaProperties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class ProcessParamValueKafkaTopicConfig {
    private final NotificationKafkaProperties properties;

    @Bean
    public KafkaAdmin processKafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapUrl());
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic processTopic() {
        NotificationKafkaProperties.@NotNull Process process = properties.getProcess();
        return new NewTopic(process.getTopic(), process.getNumberPartitions(), process.getReplicationFactor());
    }
}
