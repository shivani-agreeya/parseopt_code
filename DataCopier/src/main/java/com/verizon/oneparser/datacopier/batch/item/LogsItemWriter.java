package com.verizon.oneparser.datacopier.batch.item;

import com.verizon.oneparser.avroschemas.Logs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.batch.item.KeyValueItemWriter;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class LogsItemWriter extends KeyValueItemWriter<Long, Logs> {
    private KafkaTemplate<Long, Logs> kafkaTemplate;

    public LogsItemWriter(KafkaTemplate<Long, Logs> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        setItemKeyMapper(Logs::getId);
    }

    @Override
    protected void writeKeyValue(Long key, Logs value) {
        String topic = StringUtils.isEmpty(kafkaTemplate.getDefaultTopic()) ?
                value.getFiletype().toString() : kafkaTemplate.getDefaultTopic();
        if (this.delete) {
            log.info("Removing log message from topic:" + topic
                    + ", logId:" + value.getId() + ", fileName: " + value.getFileName());
            kafkaTemplate.send(topic, key, null); //topic = fileType
        } else {
            log.info("Sending log message to topic:" + topic
                    + ", logId:" + value.getId() + ", fileName: " + value.getFileName());
            kafkaTemplate.send(topic, key, value);
        }
    }

    @Override
    protected void init() {
    }
}
