package com.verizon.oneparser.datacopier.batch.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
@Configuration
@Validated
@ConfigurationProperties(prefix = "data.job")
public class JobProperties {
    @NotNull @NotEmpty
    private String jobName;
    @NotNull @NotEmpty
    private String stepName;
    @NotNull
    private Boolean enabled = true;
    @NotNull
    private Boolean processServerUpload = false;
    @NotNull
    private Boolean processRmOnly = false;
    @NotNull @NotEmpty
    private String srcServerUploadDir;
    @NotNull @NotEmpty
    private String srcFtpDir;
    @NotNull @NotEmpty
    private String srcFtpDirDrmDml;
    @NotNull @NotEmpty
    private String ftpOutputPath;
    @NotNull @NotEmpty
    private String ftpProcessedPath;
    @NotNull @NotEmpty
    private String ftpRootMetricProcessedPath;
    @NotNull @NotEmpty
    private String ftpFailedPath;
    @NotNull @NotEmpty
    private String dbSavePath;
    @NotNull @NotEmpty
    private String dbFailPath;
    @NotNull @NotEmpty
    private String hdfsOutputPath;
    @NotNull @NotEmpty
    private String hdfsUri;
    @NotNull
    private Integer dmUserDummy;
    @NotNull
    private Integer dmUserDrm;
    @NotNull
    private Integer dmUserDml;
    @NotNull
    private Integer dmUserSig;
    private String env;
    @NotNull
    private Long fixedRate;
    private String gwApiUrl;
}
