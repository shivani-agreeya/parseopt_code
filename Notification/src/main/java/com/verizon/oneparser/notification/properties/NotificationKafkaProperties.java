package com.verizon.oneparser.notification.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "notification.kafka")
public class NotificationKafkaProperties {
    @NotNull
    private Logs logs;
    @NotNull
    private Process process;
    @NotBlank
    private String bootstrapUrl;
    @NotBlank
    private String registryUrl;

    @NotNull
    private ErrorHandler errorHandler;

    @Data
    public static class ErrorHandler {
        @NotNull
        private Long retryInterval;
        @NotNull
        @Min(2)
        private Long retryMaxAttempt;
    }

    @Data
    public static class Logs {
        @NotBlank
        private String topic;
        @NotNull
        private Integer numberPartitions;
        @NotNull
        private Short replicationFactor;
        @NotBlank
        private String replicationGroupId;
    }

    @Data
    public static class Process {
        @NotBlank
        private String topic;
        @NotNull
        private Integer numberPartitions;
        @NotNull
        private Short replicationFactor;
        @NotBlank
        private String replicationGroupId;
    }
}
