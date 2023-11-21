package com.verizon.oneparser.broker

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.avroschemas.Logs
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.utils.DateUtils
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.functions.{from_confluent_avro, to_confluent_avro}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import com.verizon.oneparser.utils.dataframe.ExtraDataFrameOperations.implicits._

import scala.util.Try

/**
 * KafkaSink
 *
 * Provides kafka read/write and Avro encode/decode functionality
 */
case object KafkaSink extends LazyLogging {
  def apply(spark: SparkSession, config: CommonConfigParameters, topic: String = null): KafkaSink = {
    spark.udf.register("serializeLong", (s: Long) => new LongSerializer().serialize(null, s.longValue()))
    unit(spark, config, null)
  }

  def unit(spark: SparkSession, config: CommonConfigParameters, topic: String = null): KafkaSink =
    KafkaSink(spark, config, topic, null)
}

case class KafkaSink(spark: SparkSession, config: CommonConfigParameters, topic: String, df: DataFrame) extends LazyLogging {

  import spark.implicits._

  /**
   * Reads predefined topic from kafka source
   * and decodes Avro binary data by topic schema after read operation
   *
   * @return KProcess instance with non streaming DataFrame
   */
  def read: KafkaSink = {
    read(topic)
  }

  /**
   * Reads specified topic from kafka source
   * and decodes Avro binary data by topic schema after read operation
   *
   * @param topic specified topic name
   * @return KProcess instance with non streaming DataFrame
   */
  def read(topic: String): KafkaSink = {
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
    KafkaSink(spark, config, topic, df)
      .fromAvro(topic)
  }

  /**
   * Reads predefined topic from kafka source
   * and decodes Avro binary data by topic schema after read operation
   *
   * @return KProcess instance with streaming DataFrame
   */
  def readStream: KafkaSink = {
    readStream(topic)
  }

  /**
   * Reads specified topic from kafka source
   * and decodes Avro binary data by topic schema after read operation
   *
   * @param topic specified topic name
   * @return KProcess instance with streaming DataFrame
   */
  def readStream(topic: String, startingOffsets: String = config.STARTING_OFFSETS): KafkaSink = {
    readStreamFilteredByStatus(topic, Seq.empty, startingOffsets)
  }

  def readStreamFilteredByStatus(topic: String, withStatuses: Seq[String], startingOffsets: String = config.STARTING_OFFSETS): KafkaSink = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
      .option("subscribe", topic)
      .option("kafka.consumer.commit.groupid", config.KAFKA_GROUP_ID)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
      .option("enable.auto.commit", false)
      .option("failOnDataLoss", false)
      .option("minPartitions", config.KAFKA_MIN_PARTITIONS)
      .load()
    KafkaSink(spark, config, topic, df)
      .fromAvroFilteredByStatuses(topic, withStatuses)
  }

  /**
   * Writes DataFrame to predefined topic of kafka source
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @return KProcess instance with non streaming DataFrame
   */
  def write: KafkaSink = {
    write(topic)
  }

  /**
   * Writes DataFrame to specified topic of kafka source
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @param topic specified topic name
   * @return KProcess instance with non streaming DataFrame
   */
  def write(topic: String): KafkaSink = {
    toAvro(df, topic)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
      .option("topic", topic)
      .save()
    KafkaSink(spark, config, topic, df)
  }

  /**
   * Writes DataFrame to predefined topic of kafka source
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @return KProcess instance with streaming DataFrame
   */
  def writeStream: KafkaSink = {
    writeStream(topic)
  }

  /**
   * Writes DataFrame to predefined topic of kafka source
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @param topic specified topic name
   * @return KProcess instance with streaming DataFrame
   */
  def writeStream(topic: String): KafkaSink = {
    toAvro(df, topic)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
      .option("topic", topic)
      .start()
    KafkaSink(spark, config, topic, df)
  }

  /**
   * Writes DataFrame to predefined topic of kafka source in batch mode
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @param topics specified topic names
   * @return KProcess instance with streaming DataFrame
   */
  def writeStreamBatch(topics: String*): KafkaSink = {
    df
      .writeStream
      .option("checkpointLocation", checkpoint(topics.mkString("_")))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.cache()
        for (t <- topics)
          toAvro(fieldOrder(batchDF).setNullableStateExcept("id"), t)
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
            .option("topic", t)
            .save()
        batchDF.unpersist()
      }.start()
    KafkaSink(spark, config, null, df)
  }

  def fieldOrder(df: DataFrame) =
    df.select( $"dmUser",  $"creationTime",  $"lastLogTime",  $"fileName",  $"size",  $"mdn",  $"imei",  $"id",  $"firstname",  $"lastname",  $"isinbuilding",  $"hasgps",  $"emailid",  $"status",  $"modelName",  $"fileLocation",  $"esDataStatus",  $"esRecordCount",  $"importstarttime",  $"importstoptime",  $"filetype",  $"startLogTime",  $"updatedTime",  $"failureInfo",  $"logfilestatusforlv",  $"lvupdatedtime",  $"lverrordetails",  $"statusCsvFiles",  $"csvUpdatedTime",  $"csvErrorDetails",  $"technology",  $"seqStartTime",  $"seqEndTime",  $"hiveStartTime",  $"hiveEndTime",  $"esStartTime",  $"esEndTime",  $"lvfilestarttime",  $"missingVersions",  $"lvPick",  $"lvStatus",  $"lvparseduserid",  $"lvlogfilecnt",  $"processingServer",  $"reportsHiveTblName",  $"compressionstatus",  $"eventsCount",  $"ftpstatus",  $"userPriorityTime",  $"reprocessedBy",  $"pcapMulti",  $"pcapSingleRm",  $"pcapSingleUm",  $"cpHdfsTime",  $"cpHdfsStartTime", $"cpHdfsEndTime", $"hiveDirEpoch",  $"ondemandBy", $"carrierName", $"odlvUserId", $"odlvUpload", $"odlvOnpStatus", $"odlvLvStatus", $"zipStartTime", $"zipEndTime", $"logcodes", $"triggerCount")

  /**
   * Writes DataFrame to predefined topic of kafka source in batch mode
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @param topics specified topic names
   * @return KProcess instance with streaming DataFrame
   */
  def writeBatch(df: DataFrame, topics: String*): Unit = {
    for (t <- topics)
      toAvro(fieldOrder(df), t)
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
        .option("topic", t)
        .save()
  }

  /**
   * Writes DataFrame to predefined topic of kafka source in batch mode with preceding transformation
   * and encodes DataFrame to Avro binary data for topic schema before write operation
   *
   * @param transformation transformation to apply to a batch DataFrame
   * @param topics specified topic names
   * @return KProcess instance with streaming DataFrame
   */
  def writeStreamBatch(transformation: DataFrame => DataFrame, topics: String*): KafkaSink = {
    df
      .writeStream
      .option("checkpointLocation", checkpoint(topics.mkString("_")))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.cache()
        val transformedDF = transformation(batchDF)
        for (t <- topics)
          toAvro(transformedDF, t)
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS)
            .option("topic", t)
            .save()
        batchDF.unpersist()
      }.start()
    KafkaSink(spark, config, null, df)
  }

  def writeStreamBatchRun(transformation: DataFrame => Array[String], topics: String*): KafkaSink = {
    df
      .writeStream
      .option("checkpointLocation", checkpoint(topics.mkString("_")))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val startTime = System.currentTimeMillis()
        logger.warn(s"foreachBatch - Batch ${batchId} Start time: ${DateUtils.millisToTimestamp(startTime)}.")
        batchDF.cache()
        val logFileNames = transformation(batchDF)
        batchDF.unpersist()
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime;
        val averageTime = if (logFileNames != null && logFileNames.size != 0) elapsedTime/logFileNames.size else 0
        logger.warn(s"foreachBatch - Batch ${batchId} End time: ${DateUtils.millisToTimestamp(endTime)}.")
        logger.warn(s"foreachBatch - Batch ${batchId}; Elapsed time: ${elapsedTime} ms.; Batch size: ${logFileNames.size}; Average elapsed time ${averageTime}; Processed: ${logFileNames.mkString(", ")}.")
      }.start()
    KafkaSink(spark, config, null, df)
  }

  /**
   * Performs transform operation on DataFrame
   *
   * @param f transform function
   * @return KProcess instance with streaming DataFrame
   */
  def transform(f: DataFrame => DataFrame): KafkaSink = {
    KafkaSink(spark, config, topic, f(df))
  }

  /**
   * Performs console output for current non streaming DataFrame
   *
   * @return Unit
   */
  def debug: Unit = {
    df.write
      .format("console")
      .option("truncate", false)
      .save()
  }

  /**
   * Performs console output for current streaming DataFrame
   *
   * @return StreamingQuery
   */
  def debugStream: StreamingQuery = {
    df.writeStream
      .format("console")
      .option("truncate", false)
      .queryName("DebugLogsDF")
      .start()
  }

  /**
   * Returns current DataFrame
   *
   * @return current DataFrame
   */
  def getDataFrame = df

  private def fromAvro(topic: String): KafkaSink = {
    fromAvroFilteredByStatuses(topic, Seq.empty)
  }

  private def fromAvroFilteredByStatuses(topic: String, withStatuses: Seq[String]): KafkaSink = {
    val data = df.select(
      from_confluent_avro($"value", valueRegistryConfig(topic)) as "message")
      .select("message.*").filter(lit(withStatuses.isEmpty) || $"status".isin(withStatuses: _*))
    KafkaSink(spark, config, topic, data)
  }

  private def toAvro(df: DataFrame, topic: String): DataFrame = {
    val data = if (Try(df("logs")).isSuccess && Try(df("logsExtended")).isSuccess) df.select($"logs.*") else df
    data.select(
      $"id" as "key",
      to_confluent_avro(
        struct(data.columns.head, data.columns.tail: _*),
        valueRegistryConfig(topic)
      ) as 'value)
      .selectExpr("CAST(serializeLong(key) AS BINARY) as key", "CAST(value AS BINARY) as value")
  }

  private def valueRegistryConfig(topic: String) = {
    getRegistryConfigForTopic(topic) +
      (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
        SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> classOf[Logs].getCanonicalName,
        SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> classOf[Logs].getPackage.toString
      )
  }

  private def getRegistryConfigForTopic(topic: String) = {
    Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> config.SCHEMA_REGISTRY_URL,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> config.PARAM_VALUE_SCHEMA_REGISTRY_ID,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic
    )
  }

  private def checkpoint(name: String) = s"${config.CHECKPOINT_LOCATION}/${checkpointEndSeg(name)}"

  private def checkpointEndSeg(name: String) = if (name == null) s"write_" else s"write_${name}"
}
