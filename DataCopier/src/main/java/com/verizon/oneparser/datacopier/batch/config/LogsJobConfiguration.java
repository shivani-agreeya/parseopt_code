package com.verizon.oneparser.datacopier.batch.config;

import com.verizon.oneparser.avroschemas.Logs;
import com.verizon.oneparser.datacopier.batch.config.properties.JobProperties;
import com.verizon.oneparser.datacopier.batch.config.properties.MetastoreProperties;
import com.verizon.oneparser.datacopier.batch.datatype.LogMetadata;
import com.verizon.oneparser.datacopier.batch.item.LogsItemProcessor;
import com.verizon.oneparser.datacopier.batch.item.LogsItemReader;
import com.verizon.oneparser.datacopier.batch.item.LogsItemWriter;
import com.verizon.oneparser.datacopier.batch.item.GwLogsItemReader;
import com.verizon.oneparser.datacopier.persistence.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.KeyValueItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class LogsJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final KafkaTemplate<Long, Logs> kafkaTemplate;
    private final JobProperties jobProperties;
    private final MetastoreProperties metastoreProperties;

    private final LogRepository logRepository;

    @Autowired(required = false)
    private CuratorFramework client;

    @Bean
    @Profile("!zk-store")
    public ConcurrentMetadataStore propertiesPersistingMetadataStore() {
        log.info("Initializing PropertiesPersistingMetadataStore");
        PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
        metadataStore.setBaseDirectory(metastoreProperties.getPropertiesPersistingDir());
        return metadataStore;
    }

    @ConditionalOnProperty(prefix="data.job", name="gw_api_url")
    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @StepScope
    @Bean
    ItemReader logsItemReader(ConcurrentMetadataStore metadataStore) {
        ItemReader itemReader = StringUtils.isEmpty(jobProperties.getGwApiUrl()) ?
                new LogsItemReader(metadataStore, metastoreProperties, client, logRepository, jobProperties) :
                new GwLogsItemReader(metadataStore, metastoreProperties, client, restTemplate(), jobProperties);
        return itemReader;
    }

    @StepScope
    @Bean
    LogsItemProcessor logsItemProcessor(ConcurrentMetadataStore metadataStore) {
        return new LogsItemProcessor(jobProperties, logRepository, metadataStore);
    }

    @Bean
    KeyValueItemWriter<Long, Logs> kafkaItemWriter() {
        return new LogsItemWriter(kafkaTemplate);
    }

    @Bean
    public Job logsJob(JobRepository jobRepository, Step startStep) {
        return this.jobBuilderFactory.get(jobProperties.getJobName())
                .incrementer(new RunIdIncrementer())
                .repository(jobRepository)
                .start(startStep)
                .build();
    }

    @Bean
    public Step startStep(PlatformTransactionManager transactionManager,
                          ConcurrentMetadataStore metadataStore,
                  @Value("${batch.commit-interval:10}") Integer batchCommitInterval) {
        return this.stepBuilderFactory.get(jobProperties.getStepName())
            .transactionManager(transactionManager)
            .<LogMetadata, Logs> chunk(batchCommitInterval)
            .reader(logsItemReader(metadataStore))
            .processor(logsItemProcessor(metadataStore))
            .writer(kafkaItemWriter())
            .exceptionHandler((repeatContext, throwable) ->
                    log.error("Exception during processing. DB changes would be rolled back.", throwable))
            .build();
    }
}
