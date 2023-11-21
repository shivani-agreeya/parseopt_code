package com.verizon.oneparser.writesequencetohdfs.kafka

import com.verizon.oneparser.avroschemas.Logs
import com.verizon.oneparser.writesequencetohdfs.LogStatus
import com.verizon.oneparser.writesequencetohdfs.processor.LogsProcessor
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.Body

@KafkaListener(
        groupId = "\${app.config.topics.groupId}",
        offsetReset = OffsetReset.LATEST,
        offsetStrategy = OffsetStrategy.SYNC
)
class LogsListener(
        private val notificationService: LogsProducer,
        private val logsProcessor: LogsProcessor
) {

    @Topic("\${app.config.topics.listen}")
    fun receive(@Body value: Logs) {
        preprocess(value)
        notificationService.sendNotification(value.getId(), value)
        logsProcessor.process(value)
        if (value.getStatus() != LogStatus.DLF_DOES_NOT_EXIST) {
            notificationService.sendNotification(value.getId(), value)
            notificationService.sendHive(value.getId(), value)
        }
    }

    private fun preprocess(logs: Logs) {
        logs.setStatus(LogStatus.SEQ_IN_PROGRESS)
        logs.setSeqStartTime(System.currentTimeMillis())
    }
}
