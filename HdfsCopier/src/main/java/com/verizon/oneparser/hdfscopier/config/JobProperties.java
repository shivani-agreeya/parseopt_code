package com.verizon.oneparser.hdfscopier.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

@Data
@Configuration
@Validated
@ConfigurationProperties(prefix = "hdfs.job")
public class JobProperties {
    @NotNull @NotBlank
    private String env;
    @NotNull
    private Long fixedRate;
    @NotNull @NotBlank
    private String dbgwUrl;
    @NotNull @NotBlank @Pattern(regexp = "(all|dlf|sig|sig_dlf|dml_dlf|drm_dlf|q_dlf|nondmat)")
    private String dbgwFiletype;
    @NotNull
    private Integer dbgwLimit;

    private Boolean distCp = false;

    private String sourceUri;
    @NotNull @NotBlank
    private String hdfsUri;
    @NotNull @NotBlank
    private String hdfsOutputPath;

    private String nsTopic;
    private Boolean updateDb = false;

    private String pathToCoreSite;
    private String pathToHdfsSite;
}
