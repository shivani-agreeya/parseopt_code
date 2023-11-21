package com.verizon.oneparser.notification.process.kafka.config;

import com.verizon.oneparser.avroschemas.ProcessParamValues;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.RequiredArgsConstructor;
import com.verizon.oneparser.notification.properties.NotificationKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class ProcessParamValueKafkaConsumerConfig {
    private final NotificationKafkaProperties properties;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, ProcessParamValues> processKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, ProcessParamValues> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(processConsumerFactory());
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(
                properties.getErrorHandler().getRetryInterval(),
                properties.getErrorHandler().getRetryMaxAttempt() - 1)));
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, ProcessParamValues> processConsumerFactory(){
        return new DefaultKafkaConsumerFactory<>(processConsumerConfig());
    }

    @Bean
    public Map<String, Object> processConsumerConfig() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapUrl());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProcess().getReplicationGroupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        configProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put("schema.registry.url", properties.getRegistryUrl());
        return configProps;
    }
}
