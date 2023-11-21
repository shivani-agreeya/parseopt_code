package com.verizon.oneparser.broker

import java.util.Properties

import com.verizon.oneparser.avroschemas.Logs
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

import com.verizon.oneparser.config.CommonConfigParameters
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{Dataset, Row}

class Producer(val spark: SparkSession, val commonConfigParameters: CommonConfigParameters) {
  spark.udf.register("serializeLong", (s: Long) => new LongSerializer().serialize(null, s.longValue()))
  import spark.implicits._

  def send(data: Dataset[Row], topic: String) = {
    toAvro(data, topic)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", commonConfigParameters.BOOTSTRAP_SERVERS)
      .option("topic", topic)
      .save()
  }

  private def toAvro(df: DataFrame, topic: String): DataFrame = {
    df.select(
      $"id" as "key",
      to_confluent_avro(
        struct(df.columns.head, df.columns.tail: _*),
        valueRegistryConfig(topic)
      ) as 'value)
      .selectExpr("CAST(serializeLong(key) AS BINARY) as key", "CAST(value AS BINARY) as value")
  }

  private def valueRegistryConfig(topic: String) = {
    Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> commonConfigParameters.SCHEMA_REGISTRY_URL,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> commonConfigParameters.PARAM_VALUE_SCHEMA_REGISTRY_ID,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> classOf[Logs].getCanonicalName,
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> classOf[Logs].getPackage.toString
    )
  }
}

class KVProducer[K, V](val config: CommonConfigParameters) {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.BOOTSTRAP_SERVERS)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  props.put("schema.registry.url", config.SCHEMA_REGISTRY_URL)
  props.put("schema.reflection", "true")
  props.put("acks", "all")

  private val producer = new KafkaProducer[K, V](props)

  def send(key: K, value: V, topic: String): Unit = {
    val record = new ProducerRecord[K, V](topic, key, value)
    producer.send(record)
    producer.flush()
  }
}
