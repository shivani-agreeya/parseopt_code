package com.verizon.oneparser.hdfscopier.job;

import com.verizon.oneparser.avroschemas.Logs;
import com.verizon.oneparser.hdfscopier.config.JobProperties;
import com.verizon.oneparser.hdfscopier.persistence.entity.Log;
import com.verizon.oneparser.hdfscopier.persistence.repository.LogRepository;
import com.verizon.oneparser.hdfscopier.web.DbGwResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HdfsCopyJob {
    private final JobProperties jobProperties;
    private final RestTemplate restTemplate;
    private final KafkaTemplate<Long, Logs> kafkaTemplate;
    private final LogRepository repository;

    private Configuration hadoopConf;

    @PostConstruct
    private void init() {
        hadoopConf = new Configuration();
        hadoopConf.addResource(new Path(jobProperties.getPathToCoreSite()));
        hadoopConf.addResource(new Path(jobProperties.getPathToHdfsSite()));
    }

    public void execute() {

        DbGwResponse dbGwResponse = getLogFilesFromDbGw();

        if(dbGwResponse != null && CollectionUtils.isNotEmpty(dbGwResponse.getResp())) {
            dbGwResponse.getResp().stream().forEach(item -> sendLogMessageToKafka(item, false, false));

            List<Long> result = copyLogFilesToHdfs(dbGwResponse);
            log.info("HDFS copy is succeeded for log files with ids " + result);

            for (DbGwResponse.DbGwItemReponse item : dbGwResponse.getResp()) {
                boolean successful = result.contains(item.getId());
                //update DB directly only if update_db=true or ns_topic is not specified
                if (Boolean.TRUE.equals(jobProperties.getUpdateDb()) ||
                        StringUtils.isEmpty(jobProperties.getNsTopic())) {
                    updateLogsInDb(item, successful);
                }
                sendLogMessageToKafka(item, successful, true);
            }
        }
    }

    private void updateLogsInDb(DbGwResponse.DbGwItemReponse item, boolean successful) {
        Log logEntity = new Log(item.getId(),
                item.getDmUser(),
                item.getCreationTime() == null ? new Timestamp(System.currentTimeMillis()) : item.getCreationTime(),
                item.getFileName(),
                item.getSize(),
                successful ? "Moved to HDFS" : "Failed Moved to HDFS",
                item.getFileLocation(),
                successful ? "Under Processing" : "failed",
                item.getFiletype(),
                item.getUpdatedTime() == null ? new Timestamp(System.currentTimeMillis()) : item.getUpdatedTime(),
                new Timestamp(System.currentTimeMillis()),
                StringUtils.EMPTY,
                successful ? "READY" : "OP FAILED",
                jobProperties.getEnv(),
                Boolean.TRUE.equals(item.getCompressionstatus()));

        log.info("Updating log final status in DB to status " + logEntity.getStatus());
        repository.save(logEntity);
    }

    private void sendLogMessageToKafka(DbGwResponse.DbGwItemReponse item, boolean successful, boolean finished) {
        log.info("Transforming " + item + " to Avro type");
        Logs logs = new Logs();
        logs.setId(item.getId());
        logs.setCreationTime(item.getCreationTime() == null ? System.currentTimeMillis() : item.getCreationTime().getTime());
        logs.setFileName(getFileName(item.getFileName(), item.getCompressionstatus()));

        logs.setFileLocation(item.getFileLocation());
        logs.setFiletype(item.getFiletype());
        logs.setSize(item.getSize());
        logs.setDmUser(item.getDmUser());
        logs.setUpdatedTime(item.getUpdatedTime() == null ? System.currentTimeMillis() : item.getUpdatedTime().getTime());
        logs.setOdlvUpload(false);
        logs.setOdlvUserId(null);
        logs.setCompressionstatus(Boolean.TRUE.equals(item.getCompressionstatus()));

        if (!finished) { // for start event only
            logs.setCpHdfsStartTime(System.currentTimeMillis());
        } else { // for end event only
            logs.setStatus(successful ? "Moved to HDFS" : "Failed Moved to HDFS");
            logs.setEsDataStatus(successful ? "Under Processing" : "failed");
            logs.setLvPick(successful ? "READY" : "OP FAILED");
            logs.setCpHdfsEndTime(System.currentTimeMillis());
        }
        if (StringUtils.isNotEmpty(jobProperties.getNsTopic())) {
            log.info("Sending log message to ns topic:" + jobProperties.getNsTopic()
                    + ", logId:" + logs.getId() + ", fileName: " + logs.getFileName() + ", status: " + logs.getStatus() + ", compressionstatus: " + logs.getCompressionstatus());
            kafkaTemplate.send(jobProperties.getNsTopic(), logs.getId(), logs);
        }

        if (successful && finished) {
            log.info("Sending log message to spark topic:" + kafkaTemplate.getDefaultTopic()
                    + ", logId:" + logs.getId() + ", fileName: " + logs.getFileName() + ", status: " + logs.getStatus() + ", compressionstatus: " + logs.getCompressionstatus());
            kafkaTemplate.sendDefault(logs.getId(), logs);
        }
    }

    private List<Long> copyLogFilesToHdfs(DbGwResponse dbGwResponse) {
        log.info("Moving files to HDFS");
        if (Boolean.TRUE.equals(jobProperties.getDistCp())) {
            return distCopyLogFilesToHdfs(dbGwResponse);
        } else {
            return localCopyLogFilesToHdfs(dbGwResponse);
        }
    }

    private List<Long> distCopyLogFilesToHdfs(DbGwResponse dbGwResponse) {
        log.info("Copy distributed files to HDFS");
        Map<String, List<Pair<Long, Path>>> sourceMap = dbGwResponse.getResp().stream()
                .collect(Collectors.groupingBy(DbGwResponse.DbGwItemReponse::getFileLocation,
                        Collectors.mapping(item -> Pair.of(item.getId(),
                                new Path(jobProperties.getSourceUri() + item.getFileLocation() + "/" + getFileName(item.getFileName(), item.getCompressionstatus()))),
                                Collectors.toList())));
        log.info("sourceMap:"+sourceMap);

        List<Long> successfulIds = new ArrayList<>();

        for (Map.Entry<String, List<Pair<Long, Path>>> entry : sourceMap.entrySet()) {

            List<Pair<Long, Path>> sourceIds = entry.getValue();
            List<Path> sourcePaths = sourceIds.stream().map(Pair::getRight).collect(Collectors.toList());
            Path targetPath = buildHdfsOutputPath(entry.getKey());
            log.info("targetPath: " + targetPath);
            log.info("sourcePaths: " + sourcePaths);
            boolean sucessful = false;
            try {
                DistCpOptions distCpOption = new DistCpOptions(sourcePaths, targetPath);
                distCpOption.setOverwrite(true);
                distCpOption.setIgnoreFailures(true);
                //distCpOption.setMaxMaps(10)
                log.info("distCpOption:"+distCpOption);
                JobConf jobConf = new JobConf(hadoopConf);
                jobConf.setJobName("OneParser HDFS Copier");
                //jobConf.setNumMapTasks(10)
                Job distcpobj = new DistCp(jobConf,distCpOption).execute();
                boolean jobStatus = distcpobj.waitForCompletion(true);

                log.info("DistCp job is awaited for completion : "+ jobStatus);
                log.info("Job object: " + distcpobj);
                log.info("DistCp job id: " + distcpobj.getJobID().toString());
                log.info("DistCp job name: " + distcpobj.getJobName());
                //log.info(distcpobj.getJobFile()+" is JOB File")
                log.info("DistCp job state " +distcpobj.getJobState());
                //log.info(distcpobj.getSchedulingInfo()+" is JOB Schedule")
                log.info("DistCp job is completed: " + distcpobj.isComplete());
        //        log.info("DistCp job number of mappers: "+ jobConf.getNumMapTasks());
                log.info("DistCp job max map attempts: "+ distcpobj.getMaxMapAttempts());
                log.info("DistCp job failureInfo: " + distcpobj.getStatus().getFailureInfo());
                sucessful = distcpobj.isSuccessful();
                log.info("DistCp job is successful: "+ sucessful);
            } catch (Exception e) {
                log.error("Failed to execute DistCp job", e);
            }

            List<Long> logIds = sourceIds.stream().map(Pair::getLeft).collect(Collectors.toList());
            if (sucessful) {
                successfulIds.addAll(logIds);
            }
        }
        return successfulIds;
    }

    private List<Long> localCopyLogFilesToHdfs(DbGwResponse dbGwResponse) {
        log.info("Copy local files to HDFS");

        List<Long> successfulIds = new ArrayList<>();

        for (DbGwResponse.DbGwItemReponse item : dbGwResponse.getResp()) {
            @NotNull String fileLocation = item.getFileLocation();
            @NotNull String fileName = getFileName(item.getFileName(), item.getCompressionstatus());
            Path targetPath = new Path(jobProperties.getHdfsOutputPath() + parseDate(fileLocation) + "/processed/" + fileName);//buildHdfsOutputPath(fileLocation);
            Path sourcePath = new Path(fileLocation + "/" + fileName);
            log.info("targetPath: " + targetPath);
            log.info("sourcePath: " + sourcePath);

            log.info("Moving "+ sourcePath + " to HDFS " + targetPath);
//            System.setProperty("HADOOP_USER_NAME", "hdfs");
            try {
                FileSystem fs = FileSystem.get(new java.net.URI(jobProperties.getHdfsUri()), hadoopConf, "hdfs");
                fs.copyFromLocalFile(false, true, sourcePath, targetPath);
                successfulIds.add(item.getId());
            } catch (Exception e) {
                log.error("Failed copyFromLocalFile to HDFS", e);
            }
        }
        return successfulIds;
    }

    private DbGwResponse getLogFilesFromDbGw() {
        log.info("Fetching records from DB Gateway");
        DbGwResponse dbGwResponse;
        switch (jobProperties.getDbgwFiletype().toLowerCase()) {
            case "all":
                log.info("Fetching all logfiles from DB Gateway");
                log.info("Fetching dlf logfiles from DB Gateway");
                dbGwResponse = restTemplate.getForObject(buildDbGwUrl("dlf"), DbGwResponse.class);
                if (CollectionUtils.isEmpty(dbGwResponse.getResp())) {
                    dbGwResponse = getNonDmatLogFilesFromDbGw();
                }
                break;
            case "nondmat":
                dbGwResponse = getNonDmatLogFilesFromDbGw();
                break;
            case "dlf":
                log.info("Fetching dlf logfiles from DB Gateway");
                dbGwResponse = restTemplate.getForObject(buildDbGwUrl("dlf"), DbGwResponse.class);
                break;
            case "dml_dlf":
                log.info("Fetching dml_dlf logfiles from DB Gateway");
                dbGwResponse = restTemplate.getForObject(buildDbGwUrl("dml_dlf"), DbGwResponse.class);
                break;
            default:
                log.info("Filetype " + jobProperties.getDbgwFiletype() + " is not supported by DB Gateway");
                dbGwResponse = new DbGwResponse(Collections.EMPTY_LIST);

        }
        log.info("dbGwResponse: " + dbGwResponse);
        return dbGwResponse;
    }

    private DbGwResponse getNonDmatLogFilesFromDbGw() {
        log.info("Fetching nondmat logfiles from DB Gateway");
        DbGwResponse dbGwResponse = restTemplate.getForObject(buildDbGwUrl("sig"), DbGwResponse.class);
        if (CollectionUtils.isEmpty(dbGwResponse.getResp())) {
            log.info("Fetching dml_dlf logfiles from DB Gateway");
            dbGwResponse = restTemplate.getForObject(buildDbGwUrl("dml_dlf"), DbGwResponse.class);
            if (CollectionUtils.isEmpty(dbGwResponse.getResp())) {
                log.info("Fetching drm_dlf logfiles from DB Gateway");
                dbGwResponse = restTemplate.getForObject(buildDbGwUrl("drm_dlf"), DbGwResponse.class);
                if (CollectionUtils.isEmpty(dbGwResponse.getResp())) {
                    log.info("Fetching q_dlf logfiles from DB Gateway");
                    dbGwResponse = restTemplate.getForObject(buildDbGwUrl("q_dlf"), DbGwResponse.class);
                }
            }
        }
        return dbGwResponse;
    }

    private String buildDbGwUrl(String fileType) { //dbgw/api/v1/logs/dlf/5/DEV
        return jobProperties.getDbgwUrl()+"/"+fileType+"/"+jobProperties.getDbgwLimit()+"/"+jobProperties.getEnv();
    }

    private Path buildHdfsOutputPath(String fileLocation) {
        return new Path(jobProperties.getHdfsOutputPath() + parseDate(fileLocation) + "/processed/");
    }

    private String parseDate(String fileLocation) {
        String date = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(fileLocation)) {
            String[] filelocationDirs = fileLocation.split("/");
            if (filelocationDirs != null && filelocationDirs.length > 1) {
                String filelocationDate = filelocationDirs[filelocationDirs.length - 2];
                if (filelocationDate != null) {
                        try {
                        Integer.parseInt(filelocationDate);
                        date = filelocationDate;
                    } catch (NumberFormatException e) {
                        log.error("Failed to parse date from file_location");
                    }
                }
            }
        }
        return date;
    }

    //DB-GW returns log filename with general extension (as filename.dlf), but in file system file is located under .zip (as filename.zip)
    private String getFileName(String fileName, Boolean compressionStatus) {
        return Boolean.TRUE.equals(compressionStatus) ? FilenameUtils.getBaseName(fileName) + ".zip" : fileName;
    }
}
