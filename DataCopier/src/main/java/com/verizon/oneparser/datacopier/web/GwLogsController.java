package com.verizon.oneparser.datacopier.web;

import com.verizon.oneparser.datacopier.batch.config.properties.JobProperties;
import com.verizon.oneparser.datacopier.web.dto.LogResponse;
import com.verizon.oneparser.datacopier.web.dto.LogResponse.LogItemReponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.ListUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.file.DefaultDirectoryScanner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.QueryParam;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
@Profile("gw-stub")
@RestController
@RequestMapping("dbgw/api/v1/logs")
@AllArgsConstructor
public class GwLogsController {

    private final JobProperties jobProperties;

    @GetMapping(produces = "application/json")
    public LogResponse getLogs(@QueryParam("id") Integer id, @QueryParam("size") Integer size) {
        List<File> serverUpload = new DefaultDirectoryScanner().listFiles(new File(jobProperties.getSrcServerUploadDir()));
        List<File> ftpDir = new DefaultDirectoryScanner().listFiles(new File(jobProperties.getSrcFtpDir()));
        List<File> ftpDirDrmDml = new DefaultDirectoryScanner().listFiles(new File(jobProperties.getSrcFtpDirDrmDml()));
        List<File> files = ListUtils.union(serverUpload, ListUtils.union(ftpDir, ftpDirDrmDml));
        int filesSize = files.size();
        if (id != null) {
            int logId = id < filesSize ? id : 0;
            File logFile = files.get(logId);
            return new LogResponse(Collections.singletonList(logItemResponse(logFile)));
        } else {
            List<File> filesRequested = size != null && size > 0 && size < filesSize ? files.subList(0, size) : files;
            return new LogResponse(filesRequested.stream().filter(f -> f.isFile()).map(logFile -> logItemResponse(logFile)).collect(Collectors.toList()));
        }
    }

    private LogItemReponse logItemResponse(File logFile) {
        return new LogItemReponse(ThreadLocalRandom.current().nextInt(0, 100_000),
                logFile.getName(), logFile.getParent(),
                new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:sss").format(new Date(System.currentTimeMillis())));
    }
}
