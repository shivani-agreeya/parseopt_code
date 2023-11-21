package com.verizon.oneparser.datacopier.batch.config;

import com.verizon.oneparser.datacopier.batch.config.properties.MetastoreProperties;
import com.verizon.oneparser.datacopier.batch.config.properties.ZookeeperProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.zookeeper.metadata.ZookeeperMetadataStore;

@Slf4j
@EnableConfigurationProperties
@Configuration
@Profile("zk-store")
@RequiredArgsConstructor
public class ZookeeperMetadataStoreConfig {
    private final ZookeeperProperties zookeeperProperties;
    private final MetastoreProperties metastoreProperties;

    @Autowired(required = false)
    private EnsembleProvider ensembleProvider;

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(RetryPolicy retryPolicy) throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        if (this.ensembleProvider != null) {
            builder.ensembleProvider(this.ensembleProvider);
        }
        else {
            builder.connectString(zookeeperProperties.getConnectString());
        }
        CuratorFramework curator = builder.retryPolicy(retryPolicy).build();
        curator.start();
        log.trace("blocking until connected to zookeeper for "
                + zookeeperProperties.getBlockUntilConnectedWait()
                + zookeeperProperties.getBlockUntilConnectedUnit());
        curator.blockUntilConnected(zookeeperProperties.getBlockUntilConnectedWait(),
                zookeeperProperties.getBlockUntilConnectedUnit());
        log.trace("connected to zookeeper");
        return curator;
    }

    @Bean
    public RetryPolicy exponentialBackoffRetry() {
        return new ExponentialBackoffRetry(zookeeperProperties.getBaseSleepTimeMs(),
                zookeeperProperties.getMaxRetries(), zookeeperProperties.getMaxSleepMs());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public ZookeeperMetadataStore zookeeperMetadataStore(CuratorFramework client) {
        log.info("Initializing ZookeeperMetadataStore");
        ZookeeperMetadataStore zookeeperMetadataStore = new ZookeeperMetadataStore(client);
        zookeeperMetadataStore.setRoot(metastoreProperties.getZookeeperRoot());
        return zookeeperMetadataStore;
    }
}
