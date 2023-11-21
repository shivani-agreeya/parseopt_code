package com.verizon.oneparser.datacopier.batch.item;

import com.verizon.oneparser.datacopier.batch.config.properties.JobProperties;
import com.verizon.oneparser.datacopier.batch.config.properties.MetastoreProperties;
import com.verizon.oneparser.datacopier.batch.datatype.LogMetadata;
import com.verizon.oneparser.datacopier.persistence.entity.Log;
import com.verizon.oneparser.datacopier.persistence.repository.LogRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.integration.file.DefaultDirectoryScanner;
import org.springframework.integration.file.DirectoryScanner;
import org.springframework.integration.file.filters.FileSystemPersistentAcceptOnceFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessagingException;
import org.springframework.util.CollectionUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class LogsItemReader extends AbstractItemStreamItemReader<LogMetadata> {
    private LogMetadata[] data;
    private AtomicInteger counter = new AtomicInteger(0);
    private FileSystemPersistentAcceptOnceFileListFilter filter;

    private final String FILE_EXTENSION_REGEX = "([^\\s]+(\\.(?i)(%s))$)";
    private final String FILE_NAME_REGEX = "(%s)";

    protected final MetastoreProperties metastoreProperties;
    protected final CuratorFramework curator;

    public LogsItemReader(ConcurrentMetadataStore metadataStore,
                          MetastoreProperties metastoreProperties,
                          CuratorFramework curator,
                          LogRepository logRepository,
                          JobProperties jobProperties) {
        this.setExecutionContextName(this.getClass().getName());
        filter = new FileSystemPersistentAcceptOnceFileListFilter(metadataStore, metastoreProperties.getMetastorePrefix());
        filter.setFlushOnUpdate(true);

        this.metastoreProperties = metastoreProperties;
        this.curator = curator;

        Set<Log> serverUploadLogFiles = Boolean.TRUE.equals(jobProperties.getProcessServerUpload()) ?
                logRepository.findByStatusAndFiletypeOrderByIdAsc("server upload done", "dlf") : Collections.emptySet();
        if (!CollectionUtils.isEmpty(serverUploadLogFiles)) {
            log.info("Searching log files in Server Upload DIR: " + jobProperties.getSrcServerUploadDir());
            Set<String> logFileNameFilters = Boolean.TRUE.equals(jobProperties.getProcessRmOnly()) ?
                    Collections.emptySet() : serverUploadLogFiles.stream().map(Log::getFileName).collect(Collectors.toSet());
            List<File> logFiles = listFiles(jobProperties.getSrcServerUploadDir(), logFileNameFilters, false, metadataStore);
            if (logFiles.isEmpty()) {
                log.info("DB has " + serverUploadLogFiles.size()
                        + " log record(s) with status 'server upload done', but Server Upload DIR "
                        + jobProperties.getSrcServerUploadDir() + " is empty.");
            }

            Map<String, Log> logFileNameMap =
                    serverUploadLogFiles.stream().collect(Collectors.toMap(Log::getFileName, Function.identity()));

            data = logFiles.stream()
                    .map(file -> new LogMetadata(logFileNameMap.get(file.getName()), file, getUserId(file.getName(), jobProperties))).toArray(LogMetadata[]::new);
            if (logFiles.size() != data.length) {
                log.info("DB logs table and Server Upload DIR are inconsistent.");
            }
        } else {
            log.info("Searching log files in FTP DIR: " + jobProperties.getSrcFtpDir() + ", " + jobProperties.getSrcFtpDirDrmDml());
            List<String> dlfLogFileNameFilters = Boolean.TRUE.equals(jobProperties.getProcessRmOnly()) ?
                    Collections.emptyList() : Arrays.asList("zip", "dlf");
            List<File> dlfLogFiles = listFiles(jobProperties.getSrcFtpDir(), dlfLogFileNameFilters, true, metadataStore);

            List<String> drmDmlLogFileNameFilters = Boolean.TRUE.equals(jobProperties.getProcessRmOnly()) ?
                    Collections.singletonList("dml_dlf") : Arrays.asList("drm_dlf", "dml_dlf", "sig_dlf");
            List<File> drmDmlLogFiles = listFiles(jobProperties.getSrcFtpDirDrmDml(), drmDmlLogFileNameFilters, true, metadataStore);

            List<File> logFiles = ListUtils.union(dlfLogFiles, drmDmlLogFiles);

            data = logFiles.stream()
                    .map(file -> new LogMetadata(null, file, getUserId(file.getName(), jobProperties))).toArray(LogMetadata[]::new);
        }
        log.info("Found " + data.length + " log files to process.");
    }

    @Nullable
    public synchronized LogMetadata read() {
        int index = this.counter.incrementAndGet() - 1;
        log.info("Reading file with index " + index + ", total number of files to read " + this.data.length);
        if (index >= this.data.length) {
            return null;
        } else {
            File logfile = this.data[index].getFile();
            log.info("Reading file " + logfile);
            boolean accepted = filter.accept(logfile);
            if (accepted) {
                log.info("Reading file " + logfile + " succeeded");
            } else {
                log.info("Reading file " + logfile + " failed, file won't be processed further");
                this.data[index].setFile(null); //when 'read'ing after returning null or reaching batch size, 'process'ing stage is started; hence don't return null here, but filter in 'process', to let other batch record be read/processed
            }
            return this.data[index];
        }
    }

    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        this.counter.set(executionContext.getInt(this.getExecutionContextKey("COUNT"), 0));
    }

    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        executionContext.putInt(this.getExecutionContextKey("COUNT"), this.counter.get());
    }

    private List<File> listFiles(@Nonnull String dir, @javax.annotation.Nullable Collection<String> fileNameFilters, boolean fileExtension, ConcurrentMetadataStore metadataStore) {
        log.info("Fetching logs from dir " + dir);
        DirectoryScanner directoryScanner = new DefaultDirectoryScanner();
        if (!CollectionUtils.isEmpty(fileNameFilters)) {
            String regex = String.format(fileExtension ? FILE_EXTENSION_REGEX : FILE_NAME_REGEX,
                    String.join("|", fileNameFilters));
            directoryScanner.setFilter(new RegexPatternFileListFilter(regex));
        }
        File directory = new File(dir);
        LogsItemUtils.createNodeIfNotExists(curator,
                metastoreProperties.getZookeeperRoot(),
                metastoreProperties.getMetastorePrefix(),
                directory);
        List<File> files = directoryScanner.listFiles(directory);
        return CollectionUtils.isEmpty(files) ? Collections.emptyList() : files;
    }

    private Integer getUserId(String fileName, JobProperties jobProperties) {
        switch (FilenameUtils.getExtension(fileName)) {
            case "drm_dlf":
                return jobProperties.getDmUserDrm();
            case "dml_dlf":
                return jobProperties.getDmUserDml();
            case "sig_dlf":
                return jobProperties.getDmUserSig();
            default:
                return jobProperties.getDmUserDummy();
        }
    }
}
