package com.verizon.oneparser.datacopier.batch.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

@Data
@Configuration
@Validated
@ConfigurationProperties(prefix = "zookeeper")
public class ZookeeperProperties {
    @NotBlank
    private String connectString;
    @NotNull
    private Integer baseSleepTimeMs;
    @NotNull
    private Integer maxSleepMs;
    @NotNull
    private Integer maxRetries;
    @NotNull
    private Integer blockUntilConnectedWait;
    @NotNull
    private TimeUnit blockUntilConnectedUnit;
}
