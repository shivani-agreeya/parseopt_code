package com.verizon.oneparser.writesequencetohdfs.kafka

import com.verizon.oneparser.avroschemas.Logs
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic

@KafkaClient
interface LogsProducer {
    @Topic("\${app.config.topics.nservice}")
    fun sendNotification(@KafkaKey key: Long, logs: Logs)
    @Topic("\${app.config.topics.hiveprocessor}")
    fun sendHive(@KafkaKey key: Long, logs: Logs)
}
