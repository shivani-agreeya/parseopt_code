package com.verizon.oneparser

import com.verizon.oneparser.broker.consumergroup.listener.KafkaOffsetListener
import com.verizon.oneparser.broker.consumergroup.utils.KafkaOffsetsUtils
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.KafkaSink
import com.verizon.oneparser.common.Constants
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.process.WriteSeqToHDFSProc
import org.apache.spark.sql.SparkSession

object WriteSequenceToHDFS extends App with LazyLogging {

  val spark = SparkSession.builder()
    .config("spark.sql.debug.maxToStringFields", 2000)
    .config("spark.debug.maxToStringFields", 2000)
    .enableHiveSupport().getOrCreate()

  val config: CommonConfigParameters = CommonConfigParameters(
    CURRENT_ENVIRONMENT = spark.conf.get("spark.CURRENT_ENVIRONMENT"),
    HDFS_FILE_DIR_PATH = spark.conf.get("spark.HDFS_FILE_DIR_PATH"),
    HDFS_URI = spark.conf.get("spark.HDFS_URI"),
    BOOTSTRAP_SERVERS = spark.conf.get("spark.BOOTSTRAP_SERVERS"),
    SCHEMA_REGISTRY_URL = spark.conf.get("spark.SCHEMA_REGISTRY_URL"),
    PARAM_VALUE_SCHEMA_REGISTRY_ID = spark.conf.get("spark.PARAM_VALUE_SCHEMA_REGISTRY_ID"),
    INGESTION_KAFKA_TOPIC = spark.conf.get("spark.INGESTION_KAFKA_TOPIC"),
    NOTIFICATION_KAFKA_TOPIC = spark.conf.get("spark.NOTIFICATION_KAFKA_TOPIC"),
    HIVE_KAFKA_TOPIC = spark.conf.get("spark.HIVE_KAFKA_TOPIC"),
    PROCESS_PARAM_VALUES_KAFKA_TOPIC = spark.conf.get("spark.PROCESS_PARAM_VALUES_KAFKA_TOPIC"),
    KAFKA_GROUP_ID = spark.conf.get("spark.KAFKA_GROUP_ID"),
    MAX_OFFSETS_PER_TRIGGER = spark.conf.get("spark.MAX_OFFSETS_PER_TRIGGER").toInt,
    STARTING_OFFSETS = spark.conf.get("spark.STARTING_OFFSETS"),
    CHECKPOINT_LOCATION = s"${spark.conf.get("spark.CHECKPOINT_LOCATION")}/${spark.conf.get("spark.WRITE_SEQ_TO_HDFS_CHECKPOINT_SEGMENT")}",
    LOG_LEVEL = spark.conf.get("spark.LOG_LEVEL"),
    WRITE_SEQ_PARTITIONS = spark.conf.get("spark.WRITE_SEQ_PARTITIONS", "1").toInt,
    KAFKA_MIN_PARTITIONS = spark.conf.get("spark.KAFKA_MIN_PARTITIONS", "1").toInt,
    LOG_FILE_DEL = spark.conf.get("spark.LOG_FILE_DEL", "false").toBoolean,
    ODLV_TRIGGER = spark.conf.get("spark.ODLV_TRIGGER", "false").toBoolean,
    LOG_VIEWER_URL = spark.conf.get("spark.LOG_VIEWER_URL", ""),
    PATH_TO_CORE_SITE = spark.conf.get("spark.PATH_TO_CORE_SITE", ""),
    PATH_TO_HDFS_SITE = spark.conf.get("spark.PATH_TO_HDFS_SITE", ""),
    PRIORITY_TIME_INTERVAL = spark.conf.get("spark.PRIORITY_TIME_INTERVAL", "6").toInt,
    IP_PARSER_JAR_SUPPORT = spark.conf.get("spark.IP_PARSER_JAR_SUPPORT", "false").toBoolean
  )

  if (config.LOG_LEVEL != null) {
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)
  }

  spark.streams.addListener(new KafkaOffsetListener())

  val topicOffsets: String = KafkaOffsetsUtils.topicOffsets(config.BOOTSTRAP_SERVERS, config.KAFKA_GROUP_ID, config.INGESTION_KAFKA_TOPIC, config.STARTING_OFFSETS)
  logger.info(s"startingOffsets: ${topicOffsets}")

  KafkaSink(spark, config)
    .readStreamFilteredByStatus(config.INGESTION_KAFKA_TOPIC, Seq(Constants.MOVED_TO_HDFS), topicOffsets)
    .transform(f = df => WriteSeqToHDFSProc(spark, config, df).preprocess)
    .writeStreamBatch(config.NOTIFICATION_KAFKA_TOPIC)
    .transform(f = df => WriteSeqToHDFSProc(spark, config, df).process)
    .writeStreamBatch(config.HIVE_KAFKA_TOPIC, config.NOTIFICATION_KAFKA_TOPIC)

  spark.streams.awaitAnyTermination()
}