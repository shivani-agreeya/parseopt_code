package com.verizon.oneparser

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.{KVProducer, KafkaSink}
import com.verizon.oneparser.common.{Constants, DBSink}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.ProcessParamValues
import com.verizon.oneparser.process.ReadSequenceToHiveProcUpdated
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc

object ReadSequenceToHiveNew extends App with ProcessLogCodes with LazyLogging {

  val spark = SparkSession.builder()
    .config("spark.sql.debug.maxToStringFields", 2000)
    .config("spark.debug.maxToStringFields", 2000)
    .config("hive.metastore.client.socket.timeout", 1800)
    .config("hive.metastore.client.connect.retry.delay", 5)
    .config("hive.server2.enable.doAs", false)
    .config("hive.server2.thrift.port", 10016)
    .config("hive.server2.transport.mode", "binary")
    .enableHiveSupport().getOrCreate()

  val config: CommonConfigParameters = CommonConfigParameters(
    CURRENT_ENVIRONMENT = spark.conf.get("spark.CURRENT_ENVIRONMENT"),
    POSTGRES_CONN_URL = spark.conf.get("spark.POSTGRES_CONN_URL"),
    POSTGRES_CONN_SDE_URL = spark.conf.get("spark.POSTGRES_CONN_SDE_URL"),
    POSTGRES_DRIVER = spark.conf.get("spark.POSTGRES_DRIVER"),
    POSTGRES_DB_URL = spark.conf.get("spark.POSTGRES_DB_URL"),
    POSTGRES_DB_USER = spark.conf.get("spark.POSTGRES_DB_USER"),
    POSTGRES_DB_PWD = spark.conf.get("spark.POSTGRES_DB_PWD"),
    HIVE_HDFS_PATH = spark.conf.get("spark.HIVE_HDFS_PATH"),
    HDFS_FILE_DIR_PATH = spark.conf.get("spark.HDFS_FILE_DIR_PATH"),
    HDFS_URI = spark.conf.get("spark.HDFS_URI"),
    DM_USER_DUMMY = spark.conf.get("spark.DM_USER_DUMMY").toInt,
    HIVE_DB = spark.conf.get("spark.HIVE_DB"),
    FINAL_HIVE_TBL_NAME = spark.conf.get("spark.FINAL_HIVE_TBL_NAME"),
    HIVE_MIN_PARTITION = spark.conf.get("spark.HIVE_MIN_PARTITION").toInt,
    LOG_GENERAL_EXCEPTION = spark.conf.get("spark.LOG_GENERAL_EXCEPTION"),
    LOG_LEVEL = spark.conf.get("spark.LOG_LEVEL"),
    ZOOKEEPER_HOSTS = spark.conf.get("spark.ZOOKEEPER_HOSTS"),
    BOOTSTRAP_SERVERS = spark.conf.get("spark.BOOTSTRAP_SERVERS"),
    MAX_OFFSETS_PER_TRIGGER = spark.conf.get("spark.MAX_OFFSETS_PER_TRIGGER").toInt,
    STARTING_OFFSETS = spark.conf.get("spark.STARTING_OFFSETS"),
    KAFKA_GROUP_ID = spark.conf.get("spark.KAFKA_GROUP_ID"),
    INGESTION_KAFKA_TOPIC = spark.conf.get("spark.INGESTION_KAFKA_TOPIC"),
    NOTIFICATION_KAFKA_TOPIC = spark.conf.get("spark.NOTIFICATION_KAFKA_TOPIC"),
    PROCESS_PARAM_VALUES_KAFKA_TOPIC = spark.conf.get("spark.PROCESS_PARAM_VALUES_KAFKA_TOPIC"),
    HIVE_KAFKA_TOPIC = spark.conf.get("spark.HIVE_KAFKA_TOPIC"),
    ES_KAFKA_TOPIC = spark.conf.get("spark.ES_KAFKA_TOPIC"),
    SCHEMA_REGISTRY_URL = spark.conf.get("spark.SCHEMA_REGISTRY_URL"),
    PARAM_VALUE_SCHEMA_REGISTRY_ID = spark.conf.get("spark.PARAM_VALUE_SCHEMA_REGISTRY_ID"),
    CHECKPOINT_LOCATION = s"${spark.conf.get("spark.CHECKPOINT_LOCATION")}/${spark.conf.get("spark.READ_SEQ_TO_HIVE_CHECKPOINT_SEGMENT")}",
    KAFKA_MIN_PARTITIONS = spark.conf.get("spark.KAFKA_MIN_PARTITIONS", "1").toInt,
    ODLV_TRIGGER = spark.conf.get("spark.ODLV_TRIGGER", "false").toBoolean,
    PATH_TO_CORE_SITE = spark.conf.get("spark.PATH_TO_CORE_SITE", ""),
    PATH_TO_HDFS_SITE = spark.conf.get("spark.PATH_TO_HDFS_SITE", "")
  )

  import spark.implicits._

  if (config.LOG_LEVEL != null) {
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)
  }

  val ppv = processParamValues

  KafkaSink(spark, config)
    .readStreamFilteredByStatus(config.HIVE_KAFKA_TOPIC, Seq(Constants.SEQ_DONE), config.STARTING_OFFSETS)
    .writeStreamBatchRun(df => ReadSequenceToHiveProcUpdated(spark, config, ppv, df).process, config.ES_KAFKA_TOPIC)

  val ppvAvro = new com.verizon.oneparser.avroschemas.ProcessParamValues()
  ppvAvro.setId(ppv.id)
  ppvAvro.setHiveStructChng("N")
  ppvAvro.setHiveCntChk("N")
  new KVProducer[Long, com.verizon.oneparser.avroschemas.ProcessParamValues](config)
    .send(ppv.id.longValue(), ppvAvro, config.PROCESS_PARAM_VALUES_KAFKA_TOPIC)

  spark.streams.awaitAnyTermination()

  private def processParamValues =
    DBSink(spark, config, "process_param_values")
      .read
      .transform(f = df => df
        .where($"key" === config.CURRENT_ENVIRONMENT)
        .orderBy(desc("id"))
        .limit(1)
        .toDF(Seq(ProcessParamValues()).toDF().columns: _*)
      )
      .getDataFrame
      .as[ProcessParamValues]
      .first()
}
