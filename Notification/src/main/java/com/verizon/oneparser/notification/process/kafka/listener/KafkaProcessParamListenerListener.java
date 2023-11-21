package com.verizon.oneparser.notification.process.kafka.listener;

import com.verizon.oneparser.avroschemas.ProcessParamValues;
import com.verizon.oneparser.notification.common.BeanUtils;
import com.verizon.oneparser.notification.process.persistence.entity.ProcessParamValuesEntity;
import com.verizon.oneparser.notification.process.persistence.repository.ProcessParamValuesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProcessParamListenerListener {
    private final ProcessParamValuesRepository valuesRepository;

    @KafkaListener(topics = "${notification.kafka.process.topic}",
            groupId = "${notification.kafka.process.replication-group-id}",
            containerFactory = "processKafkaListenerContainerFactory"
    )
    public void listen(ProcessParamValues paramValues) {
        log.info("Listen: {}", paramValues);
        ProcessParamValuesEntity processParamValuesEntity = valuesRepository.getOne(paramValues.getId());
        ModelMapper modelMapper = new ModelMapper();
        BeanUtils.updateBean(modelMapper.map(paramValues, ProcessParamValuesEntity.class), processParamValuesEntity);
        valuesRepository.save(processParamValuesEntity);
        log.info("processParamValuesEntity with id {} was updated", paramValues.getId());
    }
}
