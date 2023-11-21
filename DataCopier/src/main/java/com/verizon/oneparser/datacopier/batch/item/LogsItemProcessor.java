package com.verizon.oneparser.datacopier.batch.item;

import com.verizon.oneparser.avroschemas.Logs;
import com.verizon.oneparser.datacopier.batch.config.properties.JobProperties;
import com.verizon.oneparser.datacopier.batch.datatype.LogMetadata;
import com.verizon.oneparser.datacopier.persistence.entity.Log;
import com.verizon.oneparser.datacopier.persistence.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.zookeeper.metadata.ZookeeperMetadataStoreException;
import org.zeroturnaround.zip.ZipUtil;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class LogsItemProcessor implements ItemProcessor<LogMetadata, Logs> {
    private final JobProperties jobProperties;

    private final LogRepository logRepository;
    private final ConcurrentMetadataStore metadataStore;
    private Configuration hadoopConf;

    private ExecutionContext executionContext;

    @PostConstruct
    private void init() {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", jobProperties.getHdfsUri());
        hadoopConf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        hadoopConf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        executionContext = stepExecution.getJobExecution().getExecutionContext();
    }

    @AfterChunkError
    public void afterChunkError(ChunkContext chunkContext) {
        log.error("Error during chunk processing.");
        removeProcessedFilesFromExecutionContext();
    }

    private void persistProcessedFileInExecutionContext(String fileName) {
        Object o = executionContext.get("PROCESSED_FILES");
        if (o instanceof List<?>) {
            ((List<String>) o).add(fileName);
        } else {
            List<String> processedFiles = new ArrayList<>();
            processedFiles.add(fileName);
            executionContext.put("PROCESSED_FILES", processedFiles);
        }
    }

    private void removeProcessedFilesFromExecutionContext() {
        log.info("Removing error file references from MetadataStore.");
        Object processedFiles = executionContext.get("PROCESSED_FILES");
        if (processedFiles instanceof List<?>) {
            ((List<String>) processedFiles).forEach(fileName -> {
                try {
                    metadataStore.remove(fileName);
                } catch (ZookeeperMetadataStoreException e) {
                    log.info("Failed to remove file reference " + fileName + " from MetadataStore");
                }
            });
        }
    }

    @Override
    public Logs process(LogMetadata logMetadata) {
        log.info("Start processing " + logMetadata);
        Log logEntity = logMetadata.getLog();
        File logFile = logMetadata.getFile();
        if (logFile == null || !logFile.exists()) {
            log.info("File " + logFile + " doesn't exist or is already being processed, skipping processing stage.");
            return null;
        }
        String hdfsOutputPath = jobProperties.getHdfsOutputPath() + logMetadata.getCreatedDate();
        String ftpOutputPath = jobProperties.getFtpOutputPath() + logMetadata.getCreatedDate() + "/";
        String ftpProcessedPath = jobProperties.getFtpProcessedPath() + logMetadata.getCreatedDate() + "/";
        String ftpRootMetricProcessedPath = jobProperties.getFtpRootMetricProcessedPath() + logMetadata.getCreatedDate()+ "/";
        String ftpFailedPath = jobProperties.getFtpFailedPath() + logMetadata.getCreatedDate() + "/";
        String finalProcessedPath = ftpProcessedPath;

        double logFileSize = FileUtils.sizeOf(logFile);

        persistProcessedFileInExecutionContext(logFile.getAbsolutePath());

        String newFileName = logFile.getName().startsWith("_") ? "1" + logFile.getName() : logFile.getName();

        try {
            // move to ftpOutputPath
            java.nio.file.Path fptOutLogPath = Paths.get(ftpOutputPath + newFileName);
            if (!Files.exists(Paths.get(ftpOutputPath))) {
                log.info("Creating dir " + ftpOutputPath);
                Files.createDirectories(Paths.get(ftpOutputPath));
            }
            log.info("Moving " + logFile.getPath() + " to " + fptOutLogPath);
            Files.move(Paths.get(logFile.getPath()), fptOutLogPath, StandardCopyOption.ATOMIC_MOVE);

            // update log status in DB
            if (logEntity != null) {
                log.info("Updating log progress status in DB to status On FTP Server");
                logEntity.setFileLocation(jobProperties.getDbSavePath().replace("processed","staging") + logMetadata.getCreatedDate());
                logEntity.setStatus("On FTP Server");
                logEntity.setEsDataStatus("Under Processing");
                logEntity.setFileName(newFileName);
            } else {
                //unzip
                if (FilenameUtils.isExtension(newFileName, "zip")) {
                    log.info("Unzipping " + ftpOutputPath + newFileName + " to " + ftpOutputPath);
                    ZipUtil.unpack(new File(ftpOutputPath + newFileName), new File(ftpOutputPath));
                    Files.deleteIfExists(fptOutLogPath);
                    newFileName = newFileName.replace(".zip", ".dlf");
                    logFileSize = FileUtils.sizeOf(new File(ftpOutputPath + newFileName));
                    fptOutLogPath = Paths.get(ftpOutputPath + newFileName);
                }

                log.info("Creating new log record in DB with status On FTP Server");
                logEntity = new Log(logMetadata.getUserId(),
                        newFileName, logFileSize, "On FTP Server", "On FTP Server",
                        jobProperties.getDbSavePath().replace("processed","staging") + logMetadata.getCreatedDate(),
                        FilenameUtils.getExtension(newFileName), jobProperties.getEnv());
                logMetadata.setLog(logEntity);
            }


            //move to HDFS
            log.info("Moving "+ ftpOutputPath + newFileName + " to HDFS " + hdfsOutputPath + "/processed/" + newFileName);
	    System.setProperty("HADOOP_USER_NAME", "hdfs");
            FileSystem fs = FileSystem.get(new java.net.URI(jobProperties.getHdfsUri()), hadoopConf, "hdfs");
            fs.copyFromLocalFile(false, true, new Path(ftpOutputPath + newFileName),
                    new Path(hdfsOutputPath + "/processed/", newFileName));

            //update log status in DB
            log.info("Updating log final status in DB to status Moved to HDFS");
            logEntity.setSize(logFileSize);
            logEntity.setFileLocation(jobProperties.getDbSavePath() + logMetadata.getCreatedDate());
            logEntity.setStatus("Moved to HDFS");
            logEntity.setEsDataStatus("Under Processing");
            logEntity.setLvPick("READY");

            //need to 'flush' to ensure that in case of an exception before 'update'('save' in a 'catch') entity would have creationTime
            logRepository.saveAndFlush(logEntity);

            finalProcessedPath = FilenameUtils.isExtension(newFileName, new String[] {"drm_dlf", "dml_dlf", "sig_dlf"}) ?
                    ftpRootMetricProcessedPath : ftpProcessedPath;

            //move to ftpProcessedPath
            java.nio.file.Path ftpProcessedLogPath = Paths.get(finalProcessedPath + newFileName);
            if (!Files.exists(Paths.get(finalProcessedPath))) {
                log.info("Creating dir " + finalProcessedPath);
                Files.createDirectories(Paths.get(finalProcessedPath));
            }
            log.info("Moving " + fptOutLogPath + " to " + ftpProcessedLogPath);
            Files.move(fptOutLogPath, ftpProcessedLogPath, StandardCopyOption.ATOMIC_MOVE);

            log.info("Removing " + fptOutLogPath);
            Files.deleteIfExists(fptOutLogPath);

            //transform to avro type
            log.info("Transforming " + logEntity + " to Avro type");
            Logs logs = new Logs();
            logs.setId(logEntity.getId());
            logs.setCreationTime(logEntity.getCreationTime() == null ? System.currentTimeMillis() : logEntity.getCreationTime().getTime());
            logs.setFileName(logEntity.getFileName());
            logs.setSize(logFileSize);
            logs.setStatus(logEntity.getStatus());
            logs.setFileLocation(logEntity.getFileLocation());
            logs.setFiletype(logEntity.getFiletype());
            logs.setDmUser(logEntity.getDmUser());
            logs.setUpdatedTime(logs.getCreationTime());
            logs.setOdlvUpload(false);
            logs.setOdlvUserId(null);
            return logs;
        } catch (IOException | URISyntaxException | InterruptedException e) {
            log.error("Failed to process " + logMetadata, e.getCause());
            try {
                if (!Files.exists( Paths.get(ftpFailedPath))) {
                    log.info("Creating dir " + ftpFailedPath);
                    Files.createDirectories(Paths.get(ftpFailedPath));
                }
                if (Files.exists(Paths.get(ftpOutputPath + newFileName))) {
                    log.info("Moving " + ftpOutputPath + newFileName + " to " + ftpFailedPath + newFileName);
                    Files.move(Paths.get(ftpOutputPath + newFileName), Paths.get(ftpFailedPath + newFileName), StandardCopyOption.ATOMIC_MOVE);
                }
                if (Files.exists(Paths.get(finalProcessedPath + newFileName))) {
                    log.info("Moving " + finalProcessedPath + newFileName + " to " + ftpFailedPath + newFileName);
                    Files.move(Paths.get(finalProcessedPath + newFileName), Paths.get(ftpFailedPath + newFileName), StandardCopyOption.ATOMIC_MOVE);
                }
            } catch (IOException ex) {
                log.error("Failed to move log file to " + ftpFailedPath + newFileName);
            }

            if (logEntity != null) {
                logEntity.setUpdatedTime(new Timestamp(System.currentTimeMillis()));
                logEntity.setStatus("Failed Moved to HDFS");
                logEntity.setFailureInfo(e.getMessage());
                log.info("Updating error status in DB " + logEntity);

                logRepository.save(logEntity);
                removeProcessedFilesFromExecutionContext();
            }
            return null; //prevents further processing
        }
    }
}
