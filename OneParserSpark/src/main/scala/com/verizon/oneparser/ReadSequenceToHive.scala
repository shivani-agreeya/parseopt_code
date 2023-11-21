package com.verizon.oneparser.readseq

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.{KVProducer, KafkaSink}
import com.verizon.oneparser.broker.consumergroup.listener.KafkaOffsetListener
import com.verizon.oneparser.common.DBSink
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.ProcessParamValues
import com.verizon.oneparser.process.ReadSequenceToHiveProc
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc

object ReadSequenceToHive extends App with LazyLogging {
    val spark = SparkSession.builder()
      .config("spark.sql.debug.maxToStringFields", 2000)
      .config("spark.debug.maxToStringFields", 2000)
      .enableHiveSupport().getOrCreate()

    val config: CommonConfigParameters = CommonConfigParameters(
      CURRENT_ENVIRONMENT = spark.conf.get("spark.CURRENT_ENVIRONMENT"),
      BOOTSTRAP_SERVERS = spark.conf.get("spark.BOOTSTRAP_SERVERS"),
      SCHEMA_REGISTRY_URL = spark.conf.get("spark.SCHEMA_REGISTRY_URL"),
      PARAM_VALUE_SCHEMA_REGISTRY_ID = spark.conf.get("spark.PARAM_VALUE_SCHEMA_REGISTRY_ID"),
      INGESTION_KAFKA_TOPIC = spark.conf.get("spark.INGESTION_KAFKA_TOPIC"),
      NOTIFICATION_KAFKA_TOPIC = spark.conf.get("spark.NOTIFICATION_KAFKA_TOPIC"),
      HIVE_KAFKA_TOPIC = spark.conf.get("spark.HIVE_KAFKA_TOPIC"),
      ES_KAFKA_TOPIC = spark.conf.get("spark.ES_KAFKA_TOPIC"),
      PROCESS_PARAM_VALUES_KAFKA_TOPIC = spark.conf.get("spark.PROCESS_PARAM_VALUES_KAFKA_TOPIC"),
      KAFKA_GROUP_ID = spark.conf.get("spark.KAFKA_GROUP_ID"),
      MAX_OFFSETS_PER_TRIGGER = spark.conf.get("spark.MAX_OFFSETS_PER_TRIGGER").toInt,
      STARTING_OFFSETS = spark.conf.get("spark.STARTING_OFFSETS"),
      CHECKPOINT_LOCATION = s"${spark.conf.get("spark.CHECKPOINT_LOCATION")}/${spark.conf.get("spark.READ_SEQ_TO_HIVE_CHECKPOINT_SEGMENT")}",
      HIVE_DB = spark.conf.get("spark.HIVE_DB"),
      FINAL_HIVE_TBL_NAME = spark.conf.get("spark.FINAL_HIVE_TBL_NAME"),
      HDFS_URI = spark.conf.get("spark.HDFS_URI"),
      HIVE_HDFS_PATH = spark.conf.get("spark.HIVE_HDFS_PATH")
    )

    val listener = new KafkaOffsetListener()
    spark.streams.addListener(new KafkaOffsetListener)

    import spark.implicits._

    val ppv = processParamValues

    KafkaSink(spark, config)
      .readStream(config.HIVE_KAFKA_TOPIC)
      .transform(f = ds => ReadSequenceToHiveProc(spark, config, ppv, ds).preprocess)
      .writeStreamBatch(config.NOTIFICATION_KAFKA_TOPIC)
      .transform(f = ds => ReadSequenceToHiveProc(spark, config, ppv, ds).process)
      .writeStreamBatch(ds => ReadSequenceToHiveProc(spark, config, ppv, ds).transformHiveStructure(ds), config.ES_KAFKA_TOPIC, config.NOTIFICATION_KAFKA_TOPIC)

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
