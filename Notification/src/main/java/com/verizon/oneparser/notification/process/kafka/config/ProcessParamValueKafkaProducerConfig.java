package com.verizon.oneparser.notification.process.kafka.config;

import com.verizon.oneparser.avroschemas.ProcessParamValues;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import com.verizon.oneparser.notification.properties.NotificationKafkaProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class ProcessParamValueKafkaProducerConfig {
    private final NotificationKafkaProperties properties;

    @Bean
    public KafkaTemplate<Long, ProcessParamValues> processKafkaTemplate() {
        return new KafkaTemplate<>(processProducerFactory());
    }

    @Bean
    public ProducerFactory<Long, ProcessParamValues> processProducerFactory() {
        return new DefaultKafkaProducerFactory<>(processProducerConfig());
    }

    @Bean
    public Map<String, Object> processProducerConfig() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapUrl());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", properties.getRegistryUrl());
        return configProps;
    }
}
