package com.verizon.oneparser.datacopier.batch.item;

import com.verizon.oneparser.datacopier.batch.config.properties.JobProperties;
import com.verizon.oneparser.datacopier.batch.config.properties.MetastoreProperties;
import com.verizon.oneparser.datacopier.batch.datatype.LogMetadata;
import com.verizon.oneparser.datacopier.batch.datatype.LogResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.batch.item.ItemReader;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.filters.FileSystemPersistentAcceptOnceFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.web.client.RestTemplate;

import java.io.File;

@Slf4j
public class GwLogsItemReader implements ItemReader<LogMetadata> {

    private final String FILE_EXTENSION_REGEX = "([^\\s]+(\\.(?i)(dlf|drm_dlf|dml_dlf|sig_dlf|zip))$)";
    private ChainFileListFilter<File> filter;

    protected final MetastoreProperties metastoreProperties;
    protected final CuratorFramework curator;
    private final JobProperties jobProperties;
    private final RestTemplate restTemplate;

    private int nextIndex;
    private LogResponse logResponse;

    public GwLogsItemReader(ConcurrentMetadataStore metadataStore,
                            MetastoreProperties metastoreProperties,
                            CuratorFramework curator,
                            RestTemplate restTemplate,
                            JobProperties jobProperties) {
        filter = new ChainFileListFilter<>();
        FileSystemPersistentAcceptOnceFileListFilter fsFilter =
                new FileSystemPersistentAcceptOnceFileListFilter(metadataStore, metastoreProperties.getMetastorePrefix());
        fsFilter.setFlushOnUpdate(true);
        this.filter.addFilter(fsFilter);
        this.filter.addFilter(new RegexPatternFileListFilter(FILE_EXTENSION_REGEX));

        this.metastoreProperties = metastoreProperties;
        this.curator = curator;
        this.restTemplate = restTemplate;
        this.jobProperties = jobProperties;
        log.info("GwLogsItemReader");
    }

    @Override
    public LogMetadata read() {
        if (logResponse == null) {
            log.info("Fetching logs from GW API");
            logResponse = restTemplate.getForObject(jobProperties.getGwApiUrl(), LogResponse.class);
            log.info("logResponse: " + logResponse);
        }
        LogMetadata logMetadata = null;

        log.info("Reading file with index " + nextIndex + ", total number of files to read " + logResponse.size());
        if (nextIndex < logResponse.size()) {
            LogResponse.LogItemReponse logItem = logResponse.getItem(nextIndex);

            LogsItemUtils.createNodeIfNotExists(curator,
                    metastoreProperties.getZookeeperRoot(),
                    metastoreProperties.getMetastorePrefix(),
                    new File(logItem.getFileLocation()));

            File logFile = new File(logItem.getFileLocation(), logItem.getFileName());
            logMetadata = filter.accept(logFile) ?
                    new LogMetadata(null, logFile, logItem.getDmUser()) :
                    new LogMetadata(null, null, logItem.getDmUser());
            nextIndex++;
        }
        log.info("logMetadata: " + logMetadata);
        return logMetadata;
    }
}
