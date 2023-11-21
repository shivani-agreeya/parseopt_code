package com.verizon.oneparser.datacopier.batch.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "metadata-store")
public class MetastoreProperties {
    @NotBlank @Pattern(regexp = "^/.*")
    private String zookeeperRoot;
    @NotBlank
    private String metastorePrefix = "dmat";
    @NotBlank
    private String propertiesPersistingDir;
}
