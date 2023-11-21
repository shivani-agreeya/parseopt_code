package com.verizon.oneparser

import com.dmat.qc.parser.ParseUtils

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import com.esri.hex.HexGrid
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.{KafkaSink, KafkaUpdate, Producer}
import com.verizon.oneparser.common.{CommonDaoImpl, Constants, LVFileMetaInfo, SecurityUtil}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.{Logs, LogsData}
import com.verizon.oneparser.eslogrecord.{CreateMapRdd, CreateMapRddForSigFile, ExecuteSqlQueryForSingle, LogRecordJoinedDF}
import com.verizon.oneparser.utils.DateUtils
import net.liftweb.json.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.joda.time.DateTime
import com.verizon.oneparser.utils.dataframe.ExtraDataFrameOperations.implicits._

import scala.collection.immutable.{List, Nil, Seq}
import scala.collection.mutable.Map
import scala.util.Try

object ProcessDMATRecords extends Serializable with LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.hive.manageFilesourcePartitions", false) //to permit partitionining by keys not only of type string
      .config("spark.sql.debug.maxToStringFields", 2000)
      .config("spark.debug.maxToStringFields", 2000)
      .config("hive.metastore.client.socket.timeout", 1800)
      .config("hive.metastore.client.connect.retry.delay", 5)
      .config("hive.server2.enable.doAs", false)
      .config("hive.server2.thrift.port", 10016)
      .config("hive.server2.transport.mode", "binary")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .enableHiveSupport()
      .getOrCreate()
    val conf = spark.sparkContext.getConf
    conf.setAppName("DMATESDataIngestionProcess")
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "10000000")

    GeoSparkSQLRegistrator.registerAll(spark)

    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    implicit val mapEncoder =
      org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    implicit val elasticEncoder =
      org.apache.spark.sql.Encoders.kryo[BulkRequest]
    implicit val elasticEncoder1 =
      org.apache.spark.sql.Encoders.kryo[BulkResponse]

    val startTimeRead = System.currentTimeMillis
    logger.warn("Started reading file ==>> " + startTimeRead)
    val repartitionNo = spark.conf.get("spark.repartition")
    val writeToHdfsRepartitionNo =
      spark.conf.get("spark.writeToHdfsRepartition")
    val writeFilePayloadToHdfs = spark.conf.get("spark.PROCESSDMAT_HDFS_WRITE")
    val commonConfigParams = CommonConfigParameters(
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
      WRITE_HDFS_URI = spark.conf.get("spark.WRITE_HDFS_URI"),
      ES_HOST_PATH = spark.conf.get("spark.ES_HOST_PATH"),
      ES_PORT_NUM = spark.conf.get("spark.ES_PORT_NUM").toInt,
      DM_USER_DUMMY = spark.conf.get("spark.DM_USER_DUMMY").toInt,
      HIVE_DB = spark.conf.get("spark.HIVE_DB"),
      FINAL_HIVE_TBL_NAME = spark.conf.get("spark.FINAL_HIVE_TBL_NAME"),
      TBL_QCOMM_NAME = spark.conf.get("spark.TBL_QCOMM_NAME"),
      TBL_QCOMM2_NAME = spark.conf.get("spark.TBL_QCOMM2_NAME"),
      TBL_B192_NAME = spark.conf.get("spark.TBL_B192_NAME"),
      TBL_B193_NAME = spark.conf.get("spark.TBL_B193_NAME"),
      TBL_ASN_NAME = spark.conf.get("spark.TBL_ASN_NAME"),
      TBL_NAS_NAME = spark.conf.get("spark.TBL_NAS_NAME"),
      TBL_IP_NAME = spark.conf.get("spark.TBL_IP_NAME"),
      TBL_QCOMM5G_NAME = spark.conf.get("spark.TBL_QCOMM5G_NAME"),
      SEQ_FILE_DEL = spark.conf.get("spark.SEQ_FILE_DEL"),
      DETAILED_HIVE_JOB_ANALYSIS =
        spark.conf.get("spark.DETAILED_HIVE_JOB_ANALYSIS"),
      REPORTS_HIVE_TBL_NAME = spark.conf.get("spark.REPORTS_HIVE_TBL_NAME"),
      PROCESSING_SERVER = spark.conf.get("spark.PROCESSING_SERVER"),
      PROCESS_RM_ONLY = spark.conf.get("spark.PROCESS_RM_ONLY"),
      ES_CLUSTER_NAME = spark.conf.get("spark.es_cluster_name"),
      ES_NODE_ID = spark.conf.get("spark.es_node_id"),
      ES_PORT_ID = spark.conf.get("spark.es_port_id"),
      ES_CELLSITE_INDEX = spark.conf.get("spark.es_cellsite_index"),
      ES_CELLSITE_WEEKLY_INDEX_DATE =
        spark.conf.get("spark.cellsite_weekly_index_date"),
      PROCESS_FILE_TYPE = spark.conf.get("spark.PROCESS_FILE_TYPE"),
      BOOTSTRAP_SERVERS = spark.conf.get("spark.BOOTSTRAP_SERVERS"),
      PARAM_VALUE_SCHEMA_REGISTRY_ID = spark.conf.get("spark.PARAM_VALUE_SCHEMA_REGISTRY_ID"),
      SCHEMA_REGISTRY_URL = spark.conf.get("spark.SCHEMA_REGISTRY_URL"),
      CHECKPOINT_LOCATION =
        s"${spark.conf.get("spark.CHECKPOINT_LOCATION")}/${spark.conf.get("spark.PROCESS_DMAT_RECORDS_CHECKPOINT_SEGMENT")}",
      NOTIFICATION_KAFKA_TOPIC = spark.conf.get("spark.NOTIFICATION_KAFKA_TOPIC"),
      ES_KAFKA_TOPIC = spark.conf.get("spark.ES_KAFKA_TOPIC"),
      LOG_LEVEL = spark.conf.get("spark.LOG_LEVEL"),
      EMAIL_HOST_NAME = spark.conf.get("spark.email_host_name"),
      EMAIL_SMTP_PORT = spark.conf.get("spark.email_port").toInt,
      EMAIL_USER_NAME = spark.conf.get("spark.email_user_name"),
      EMAIL_PWD = SecurityUtil.decrypt(spark.conf.get("spark.email_pwd")),
      EMAIL_FROM = spark.conf.get("spark.email_from"),
      EMAIL_SENDER_NAME = spark.conf.get("spark.email_sender_name"),
      KAFKA_GROUP_ID = spark.conf.get("spark.KAFKA_GROUP_ID"),
      MAX_OFFSETS_PER_TRIGGER = spark.conf.get("spark.MAX_OFFSETS_PER_TRIGGER").toInt,
      STARTING_OFFSETS = spark.conf.get("spark.STARTING_OFFSETS"),
      KPI_RTP_PACKET = spark.conf.get("spark.KPI_RTP_PACKET").toBoolean,
      KPI_RTP_PACKET_BROADCAST = spark.conf.get("spark.KPI_RTP_PACKET_BROADCAST").toBoolean,
      KPI_RTP_PACKET_CACHE = spark.conf.get("spark.KPI_RTP_PACKET_CACHE").toBoolean,
      KAFKA_MIN_PARTITIONS = spark.conf.get("spark.KAFKA_MIN_PARTITIONS", "1").toInt,
      WRITE_DMAT_RECORDS_TO_ES = spark.conf.get("spark.WRITE_DMAT_RECORDS_TO_ES", "true").toBoolean,
      GEO_SHAPE_FILE_PATH = spark.conf.get("spark.GEO_SHAPE_FILE_PATH"),
      ODLV_TRIGGER = spark.conf.get("spark.ODLV_TRIGGER", "false").toBoolean,
      ONP_GNBID_URL = spark.conf.get("spark.ONP_GNBID_URL", ""),
      ONP_PCIDISTANCE_URL = spark.conf.get("spark.ONP_PCIDISTANCE_URL", ""),
      ES_USER = spark.conf.get("spark.ES_USER", ""),
      ES_PWD = spark.conf.get("spark.ES_PWD", ""),
      ES_NESTEDFIELD_CUTOFFDATE = spark.conf.get("spark.ES_NESTEDFIELD_CUTOFFDATE", "2021-07-25"),
      PATH_TO_CORE_SITE = spark.conf.get("spark.PATH_TO_CORE_SITE", ""),
      PATH_TO_HDFS_SITE = spark.conf.get("spark.PATH_TO_HDFS_SITE", "")
    )
    val checkpointLocation = commonConfigParams.CHECKPOINT_LOCATION
    val producer = new Producer(spark, commonConfigParams)
    val kafkaUpdate = new KafkaUpdate(producer)
    val esTopic = commonConfigParams.ES_KAFKA_TOPIC

    if (commonConfigParams.LOG_LEVEL != null) {
      spark.sparkContext.setLogLevel(commonConfigParams.LOG_LEVEL)
    }

    import spark.implicits._

    KafkaSink(spark, commonConfigParams)
      . readStreamFilteredByStatus(esTopic, Seq(Constants.HIVE_SUCCESS), commonConfigParams.STARTING_OFFSETS)
      .getDataFrame
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch((logsTblDs, batchId) => {
        if (logsTblDs != null && !logsTblDs.isEmpty) {
          val startTime = System.currentTimeMillis()
          logger.warn(
            s"foreachBatch - Batch ${batchId} Start time: ${DateUtils.millisToTimestamp(startTime)}.")
          logsTblDs.cache()
          var logFileNames = List.empty[String]
          var rethrowToEsTopic = false
          /*
          * SIG files (with valid logcodes) and files containing VZ logcodes should be processed twice.
          * SIG files are to be sent into "scanner" and "dmat" indexes both.
          * VZ logcode files are to be sent into "vzkpis" and "dmat" indexes both.
          * If such files are found in the batch we are throwing out into message bus all other regular files to simplify logic
          * and avoid memory overuse.
          * SIG files which need to be reprocessed should be as following:
          * - SIG file type
          * - Filename shouldn't contain sig_dlf (appears to be production workaround)
          * VZ logcode files should contain specific logcodes.
          * - For both cases triggerCount should be 0. If it has other value like 1 (others aren't likely to be) then such file already processed
          * into either "scanner" or "vzkpis" index and we should treat it like regular file type "DLF" into "dmat" index.
          * SIG files with invalid logcodes (not in predefined SIG logcodes list) should be processed into "scanner" index only.
          */
          var rethrowToEsTopicDs = logsTblDs.filter(((lower($"filetype") === Constants.CONST_SIG && !$"fileName".contains(Constants.CONST_SIG_DLF))
            || $"logcodes".isNotNull && arrays_overlap(split($"logcodes", ","), lit(Constants.LOGCODES_TRACE_AUTOTEST))) && $"triggerCount" === 0)
          if (rethrowToEsTopicDs != null && !rethrowToEsTopicDs.isEmpty) {
            rethrowToEsTopicDs = rethrowToEsTopicDs.cache()
            rethrowToEsTopic = true
            val regularFilesDs = logsTblDs.exceptAll(rethrowToEsTopicDs)
            logFileNames = process(rethrowToEsTopicDs,
              spark,
              kafkaUpdate,
              repartitionNo,
              commonConfigParams,
              writeFilePayloadToHdfs,
              writeToHdfsRepartitionNo,
              rethrowToEsTopic)
            // Rethrowing all the regular files from batch into message bus again to be able to process them
            KafkaSink(spark, commonConfigParams).writeBatch(regularFilesDs.setNullableStateExcept("id"), commonConfigParams.ES_KAFKA_TOPIC)

          } else {
            logFileNames = process(logsTblDs,
              spark,
              kafkaUpdate,
              repartitionNo,
              commonConfigParams,
              writeFilePayloadToHdfs,
              writeToHdfsRepartitionNo,
              rethrowToEsTopic)
          }
          logsTblDs.unpersist()
          val endTime = System.currentTimeMillis()
          val elapsedTime = endTime - startTime;
          val averageTime =
            if (logFileNames != null && logFileNames.size != 0)
              elapsedTime / logFileNames.size
            else 0
          logger.warn(
            s"foreachBatch - Batch ${batchId} End time: ${DateUtils.millisToTimestamp(endTime)}.")
          logger.warn(
            s"foreachBatch - Batch ${batchId}; Elapsed time: ${elapsedTime} ms.; Batch size: ${logFileNames.size}; Average elapsed time ${averageTime}; Processed: ${logFileNames
              .mkString(", ")}.")
        }
      })
      .start()

    spark.streams.awaitAnyTermination()
  }

  def process(logsTblDs: Dataset[Row],
              spark: SparkSession,
              kafkaUpdate: KafkaUpdate,
              repartitionNo: String,
              commonConfigParams: CommonConfigParameters,
              writeFilePayloadToHdfs: String,
              writeToHdfsRepartitionNo: String,
              rethrowToEsTopic: Boolean): List[String] = {
    import spark.implicits._

    try {
      val kafkaSink: KafkaSink = KafkaSink(spark, commonConfigParams)
      val startTimeRead = System.currentTimeMillis
      val fileNamesList = logsTblDs
        .select("fileName")
        .rdd
        .map(row => row(0).toString)
        .collect()
        .toList
      val dateList = logsTblDs
        .select("hiveEndTime")
        .rdd
        .map(row => DateUtils.millisToString((row(0).asInstanceOf[Long])))
        .collect()
        .toList
        .distinct
      val fileNamesStr: String = Constants.addSingleQuotes(fileNamesList)
      val testIds =
        logsTblDs.select("id").rdd.map(row => row(0).toString).collect().toList
      val datesStr
        : String = Constants.addSingleQuotes(dateList) + ",'" + Constants.fetchUpdatedDirDate + "'"
      val testIdStr = Constants.addSingleQuotes(testIds)
      val logcodes = logsTblDs
        .select("logcodes")
        .rdd
        .map(row => row(0).toString.split(","))
        .collect().flatten

      val reportsHiveTblNames = logsTblDs
        .filter($"reportsHiveTblName".isNotNull)
        .select("reportsHiveTblName")
        .rdd
        .map(row => row(0).toString)
        .collect()
        .toList

      var hiveTableNameIndexStr: String = null
      if (reportsHiveTblNames != null && reportsHiveTblNames.distinct.size == 1) {
        hiveTableNameIndexStr = reportsHiveTblNames.head
      } else {
        hiveTableNameIndexStr = Constants.getHiveTableIndex(reportsHiveTblNames)
      }

      kafkaUpdate.updateEsStatusInLogs(logsTblDs, Constants.ES_INPROGRESS)

        val outSdf: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
        val inSdf: SimpleDateFormat = new SimpleDateFormat(
          "yyyy-MM-dd hh:mm:ss")
        val hiveUpSdf: SimpleDateFormat = new SimpleDateFormat(
          "yyyy-MM-dd hh:mm:ss")
        val hiveEndTimeSdf: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")

        val hiveTableName = commonConfigParams.HIVE_DB + commonConfigParams.FINAL_HIVE_TBL_NAME + hiveTableNameIndexStr
        spark.sql("refresh table " + hiveTableName)

      val logRecordJoinedDF: LogRecordJoinedDF =
        ExecuteSqlQueryForSingle.getJoinedTable(
          fileNamesStr,
          datesStr,
          testIdStr,
          spark,
          logsTblDs
            .as[Logs]
            .map(l =>
              LogsData(
                if (l.fileName.endsWith("seq")) l.fileName
                else Constants.getSeqFromDlf(l.fileName),
                l.id.intValue(),
                outSdf.format(new Date(l.updatedTime)),
                outSdf.format(new Date(l.creationTime)),
                l.fileLocation,
                if (l.dmUser == null) commonConfigParams.DM_USER_DUMMY
                else l.dmUser,
                l.firstname,
                l.lastname,
                l.emailid,
                l.mdn,
                l.imei,
                l.technology,
                if (l.isinbuilding == null || l.isinbuilding == 0) "FALSE"
                else "TRUE",
                l.modelName,
                hiveUpSdf.format(new Date(l.updatedTime)),
                l.size.longValue(),
                l.reportsHiveTblName,
                if (l.eventsCount == null) 0 else l.eventsCount,
                hiveEndTimeSdf.format(new Date(l.hiveEndTime)),
                if (l.startLogTime != null)
                  inSdf.format(new Date(l.startLogTime))
                else "",
                if (l.lastLogTime != null)
                  inSdf.format(new Date(l.lastLogTime))
                else "",
                l.filetype,
                l.ondemandBy,
                l.reprocessedBy,
                l.carrierName,
                if (l.compressionstatus == null) false
                else l.compressionstatus,
                l.triggerCount
              ))
            .toDF(),
          logcodes,
          startTimeRead,
          fileNamesList,
          hiveTableName,
          false,
          commonConfigParams
        )
      val joinedAllTables = logRecordJoinedDF.joinedAllTables

      var startTimePartition = System.currentTimeMillis
      logger.warn("START Partioning dataframe ==>> " + startTimePartition)

      val mapRdd = if (!rethrowToEsTopic) {
        CreateMapRdd.getMapRdd(joinedAllTables,
          logRecordJoinedDF,
          repartitionNo.toInt,
          false,
          commonConfigParams)
      } else {
        CreateMapRddForSigFile.getSigFileMapRdd(joinedAllTables,
          repartitionNo.toInt,
          false,
          commonConfigParams)
      }

      logger.warn(
        "Finshed partioning dataframe ==>> " + (System.currentTimeMillis - startTimePartition))

      if (commonConfigParams.WRITE_DMAT_RECORDS_TO_ES) {
        //write to ES
        val options = Map(
          "es.nodes" -> commonConfigParams.ES_HOST_PATH,
          "es.port" -> commonConfigParams.ES_PORT_NUM.toString,
          "es.net.http.auth.user" -> commonConfigParams.ES_USER,
          "es.net.http.auth.pass" -> commonConfigParams.ES_PWD,
          "es.nodes.wan.only" -> "true",
          "es.mapping.exclude" -> "internal_index,internal_type,jsonString_WriteToHdfs",
          "es.mapping.id" -> "ObjectId",
          "es.resource.write" -> "{internal_index}/{internal_type}"
        )

        import org.elasticsearch.spark._
        mapRdd.rdd.saveToEs(options)
      } else {
        spark.time(println(mapRdd.count())) //trigger execution in test mode
      }

      if (logRecordJoinedDF.cachedDf != null) {
        println("cachedDf.unpersist()")
        logRecordJoinedDF.cachedDf.unpersist() //unpersist will cancel out persist/cache if called prior to dataset action(s), so unpersist is here
      }

      //write to HDFS
      if (writeFilePayloadToHdfs.toLowerCase == "true") {
        val bulkJsonRdd = mapRdd.mapPartitions(
          _.map(_.get("jsonString_WriteToHdfs").getOrElse().toString))
        val bulkRddDf = bulkJsonRdd.toDF("jsonString_WriteToHdfs")

        writeJsonToHDFS(bulkRddDf,
          commonConfigParams,
                          writeToHdfsRepartitionNo)
        }


      //        val mapRddVmas = CreateMapRddForVMAS.getMapRdd(joinedAllTables,
      //          logRecordJoinedDF,
      //          repartitionNo.toInt,
      //          false,
      //          commonConfigParams)
      //        mapRddVmas.rdd.saveToEs(options)

      logger.warn(
        "Finished bulk rdd partition ==>> " + (System.currentTimeMillis - startTimePartition))

      val renameSeq: (String) => String = (data: String) => {
        if (data.endsWith("seq")) Constants.getDlfFromSeq(data) else data
      }

      val renameSeqUdf = udf(renameSeq)

      /*
       * triggerCount is incremented for files to be rethrown so that they are treated like regular ones at the next batch iteration.
       * status is set to "complete" unless the file is to be rethrown. If so status is back to "Hive Success".
       */
      val logfileDf = logsTblDs.
        withColumn("triggerCount", lit(if (rethrowToEsTopic) $"triggerCount" + 1 else $"triggerCount")).
        withColumn("status", when(lit(rethrowToEsTopic) && (lower($"filetype") =!= Constants.CONST_SIG || $"logcodes".isNotNull && arrays_overlap(split($"logcodes", ","), lit(Constants.DEVICE_LOGCODES_SIG.toArray))), Constants.HIVE_SUCCESS).
          otherwise(Constants.CONST_COMPLETE)).cache()

      /*
       * Insert lvFileMetainfo. triggerCount is set for 0 for all files except ones to be rethrown.
       * Need to insert unwrapped (no-seq) filename as it's db direct call
       */
      CommonDaoImpl.insertLvFileMetainfo(logfileDf.withColumn(
        "fileName", renameSeqUdf(logsTblDs.col("fileName"))).
        select("fileName", "id", "logcodes", "triggerCount").
        filter(lit(rethrowToEsTopic) || $"triggerCount" === 0).
        collect().
        map(e => LVFileMetaInfo(e.getAs[Long]("id").toInt,
          e.getAs[String]("fileName"),
          if (!e.isNullAt(e.fieldIndex("logcodes"))) e.getAs[String]("logcodes").split(",").toList else null,
          e.getAs[Int]("triggerCount"))).toList, commonConfigParams)

      // It's ok to send wrapped (seq) filename as it's not updated by notification service
      kafkaUpdate.updateStatusInOldLogs(logfileDf)

      /*
       * Send files to be rethrown with status "Hive Success" into message bus again to process them as regular ones later.
       * Note that we need wrapped (seq) filename so that such files are processed further.
       */
      if (rethrowToEsTopic) {
        kafkaSink.writeBatch(logfileDf.filter($"status" === Constants.HIVE_SUCCESS).setNullableStateExcept("id"), commonConfigParams.ES_KAFKA_TOPIC)
      }

      logfileDf.unpersist()

      fileNamesList
    } catch {
      case e: java.net.SocketException =>
        logger.error(
          "Socket exception occured while processing files : " + e.getMessage)
        logger.error(
          "Socket exception occured while processing files : " + e
            .printStackTrace())
        if (logsTblDs != null) {
          kafkaUpdate.updateEsStatusInLogs(logsTblDs, Constants.HIVE_SUCCESS)
        }
        Nil
      case e: Exception =>
        logger.error(
          "Main Catch : ES Exception occured while executing main block into ES : ",
          e)
        println(
          "Main Catch : ES Exception occured while executing main block into ES : ",
          e)
        logger.error(
          "Main Catch : ES Exception occured while executing main block into ES : ",
          e)
        Nil
    }
  }

  def writeJsonToHDFS(jsonRecords: DataFrame,
                      commonConfigParams: CommonConfigParameters,
                      repartitionNo: String): Unit = {
    val conf = new Configuration()
    val hdfsURI = commonConfigParams.WRITE_HDFS_URI

    try {
      conf.addResource(new Path(commonConfigParams.PATH_TO_CORE_SITE))
      conf.addResource(new Path(commonConfigParams.PATH_TO_HDFS_SITE))
      val fileSystem: FileSystem = FileSystem.get(conf)
      val currentDate: DateTime = new DateTime();
      val fileAbsPath = getAbsFilePathInHdfs(currentDate, false)
      val hdfsOutputPathStr = commonConfigParams.CURRENT_ENVIRONMENT + fileAbsPath
      val filePath = hdfsURI + "/" + hdfsOutputPathStr
      val hdfsPath = new Path(filePath)

      logger.warn("creating hdfs file path")
      var fileOutputStream: FSDataOutputStream = null

      try {
        logger.warn("Get hdfs path: " + hdfsPath)
        if (fileSystem.exists(hdfsPath)) {
          logger.warn("append json file to hdfs")
          jsonRecords
            .repartition(repartitionNo.toInt)
            .write
            .mode(SaveMode.Append)
            .text(filePath)
        } else {
          fileOutputStream = fileSystem.create(hdfsPath)
          logger.warn("overwrite json file to hdfs")
          jsonRecords
            .repartition(repartitionNo.toInt)
            .write
            .mode(SaveMode.Overwrite)
            .text(filePath)
        }

      } finally {

        if (fileSystem != null) {
          //fileSystem.close()
        }
        if (fileOutputStream != null) {
          //fileOutputStream.close()
        }
      }
    } catch {
      case ge: Exception =>
        logger.error("Error writing jsondata into HDFS : " + ge.getMessage)
        println("Error writing jsondata into HDFS : " + ge.getMessage)
        logger.error("Error writing jsondata into HDFS Cause: " + ge.getCause())
        logger.error(
          "Error writing jsondata into HDFS StackTrace: " + ge
            .printStackTrace())
    }
  }

  def getAbsFilePathInHdfs(dt: DateTime, fileNameStr: Boolean): String = {
    var filePath = "";
    val month = dt.getMonthOfYear
    // gets the current month
    val hours = dt.getHourOfDay
    // gets hour of day
    val day = dt.getDayOfYear
    val year = dt.getYear
    val week = dt.getWeekOfWeekyear
    val absPath = File.separator + year + Path.SEPARATOR + month + Path.SEPARATOR + day + Path.SEPARATOR + hours
    val fileName: String = String.format("dmat_%d_%d_%d_%d_%d",
                                         Int.box(dt.getYear),
                                         Int.box(dt.getMonthOfYear),
                                         Int.box(dt.getWeekOfWeekyear),
                                         Int.box(dt.getDayOfYear),
                                         Int.box(dt.getHourOfDay));
    filePath = absPath
    if (fileNameStr) {
      filePath = fileName
    }
    filePath
  }

  import scala.collection.mutable._

  def getSysModeKPIs(
      mSysMode: Int,
      mStatusMode: Integer,
      HdrHybrid: Int): scala.collection.mutable.Map[String, Any] = {
    var systemMode: Array[String] = Array("No service",
                                          "AMPS",
                                          "CDMA",
                                          "GSM",
                                          "1xEV-DO",
                                          "WCDMA",
                                          "GPS",
                                          "GSM and WCDMA",
                                          "WLAN",
                                          "LTE",
                                          "GSM and WCDMA and LTE")
    var srvStatusMode: Array[String] = Array("No service",
                                             "Limited service",
                                             "Service available",
                                             "Limited regional service",
                                             "Power save")
    val sysModeKPIs = scala.collection.mutable.Map[String, Any]()
    try {
      if (mSysMode >= 0) {
        mSysMode match {
          case 0 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
              mStatusMode))
          case 2 =>
            if (HdrHybrid == 0) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
                mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
                mStatusMode))
            } else if (mStatusMode == 2) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
                mStatusMode), "DataRat" -> "1xEV-DO", "DataRATStatus" -> srvStatusMode(
                mStatusMode))
            } else if (mStatusMode >= 0) {
              sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
                mStatusMode), "DataRat" -> "No service", "DataRATStatus" -> srvStatusMode(
                0))
            }
          case 3 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
              mStatusMode))
          case 4 =>
            sysModeKPIs += ("VoiceRAT" -> "No service", "VoiceRATStatus" -> srvStatusMode(
              mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
              mStatusMode))
          case 5 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode), "DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
              mStatusMode))
          case 9 =>
            sysModeKPIs += ("DataRat" -> systemMode(mSysMode), "DataRATStatus" -> srvStatusMode(
              mStatusMode))
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode))
        }
      }
    } catch {
      case ge: Exception =>
        logger.error(
          "ES Exception occured while executing getSysModeKPIs for mSysMode : " + mSysMode + " Message : " + ge.getMessage)
        println(
          "ES Exception occured while executing getSysModeKPIs : " + ge.getMessage)
        logger.error(
          "ES Exception occured while executing getSysModeKPIs Cause: " + ge
            .getCause())
        logger.error(
          "ES Exception occured while executing getSysModeKPIs StackTrace: " + ge
            .printStackTrace())
    }
    sysModeKPIs
  }

  def getSysModeKPIs0x134F(
      mSysMode0: Int,
      mSysMode1: Int,
      sysIDx184E: Int,
      sysModeOperational: Int,
      EmmSubState: String,
      mStatusMode0: Integer,
      mStatusMode1: Integer,
      HdrHybrid: Int): scala.collection.mutable.Map[String, Any] = {
    var systemMode: Array[String] = Array("No service",
                                          "AMPS",
                                          "CDMA",
                                          "GSM",
                                          "1xEV-DO",
                                          "WCDMA",
                                          "GPS",
                                          "GSM and WCDMA",
                                          "WLAN",
                                          "LTE",
                                          "GSM and WCDMA and LTE")
    var srvStatusMode: Array[String] = Array("No service",
                                             "Limited service",
                                             "Service available",
                                             "Limited regional service",
                                             "Power save")
    val sysModeKPIs = scala.collection.mutable.Map[String, Any]()
    try {
      if (mSysMode0 >= 0) {
        mSysMode0 match {
          case 0 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mStatusMode0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0))
          case 2 => //System mode is CDMA
            sysModeKPIs += ("VoiceRAT" -> systemMode(mStatusMode0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0))
          case 3 => //System mode is GSM
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
              mStatusMode0))
          case 4 => //System mode is EVDO
            sysModeKPIs += ("VoiceRAT" -> "No service", "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
              mStatusMode0))
          case 5 =>
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0), "DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
              mStatusMode0))
          case 9 =>
            sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
              mStatusMode0))
            sysModeKPIs += ("VoiceRAT" -> systemMode(mSysMode0), "VoiceRATStatus" -> srvStatusMode(
              mStatusMode0))
        }
      }

      if (mSysMode0 == 2) {
        if (sysModeOperational == 1) {
          mSysMode1 match {
            case 0 =>
              (if (mSysMode0 == 2) {
                 sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
                   mStatusMode0))
               } else Nil)
            case 4 =>
              sysModeKPIs += ("DataRat" -> systemMode(mSysMode1), "DataRATStatus" -> srvStatusMode(
                mStatusMode1))
            case 9 =>
              sysModeKPIs += ("DataRat" -> systemMode(mSysMode1), "DataRATStatus" -> srvStatusMode(
                mStatusMode1))
          }
        } else {
          sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
            mStatusMode0))
        }
      } else {
        sysModeKPIs += ("DataRat" -> systemMode(mSysMode0), "DataRATStatus" -> srvStatusMode(
          mStatusMode0))
      }
    } catch {
      case ge: Exception =>
        logger.error(
          "ES Exception occured while executing getSysModeKPIs0x134F for mSysMode : " + mSysMode0 + " Message : " + ge.getMessage)
        println(
          "ES Exception occured while executing getSysModeKPIs0x134F : " + ge.getMessage)
        logger.error(
          "ES Exception occured while executing getSysModeKPIs0x134F Cause: " + ge
            .getCause())
        logger.error(
          "ES Exception occured while executing getSysModeKPIs0x134F StackTrace: " + ge
            .printStackTrace())
    }
    sysModeKPIs
  }

  def getLocValues(Mx: Double, My: Double, X: String, Y: String): Map[String, Any] = {
    val grid2 = HexGrid(2, -20000000.0, -20000000.0)
    val grid5 = HexGrid(5, -20000000.0, -20000000.0)
    val grid10 = HexGrid(10, -20000000.0, -20000000.0)
    val grid25 = HexGrid(25, -20000000.0, -20000000.0)
    val grid50 = HexGrid(50, -20000000.0, -20000000.0)
    val grid100 = HexGrid(100, -20000000.0, -20000000.0)
    val grid200 = HexGrid(200, -20000000.0, -20000000.0)
    val grid250 = HexGrid(250, -20000000.0, -20000000.0)
    val grid500 = HexGrid(500, -20000000.0, -20000000.0)
    val grid1000 = HexGrid(1000, -20000000.0, -20000000.0)
    val grid2000 = HexGrid(2000, -20000000.0, -20000000.0)
    val grid4000 = HexGrid(4000, -20000000.0, -20000000.0)
    val grid5000 = HexGrid(5000, -20000000.0, -20000000.0)
    val grid10000 = HexGrid(10000, -20000000.0, -20000000.0)
    val grid15000 = HexGrid(15000, -20000000.0, -20000000.0)
    val grid25000 = HexGrid(25000, -20000000.0, -20000000.0)
    val grid50000 = HexGrid(50000, -20000000.0, -20000000.0)
    val grid75000 = HexGrid(75000, -20000000.0, -20000000.0)
    val grid100000 = HexGrid(100000, -20000000.0, -20000000.0)

    val xVal: Double = Try(X.toDouble).getOrElse(-74.635233)
    val yVal: Double = Try(Y.toDouble).getOrElse(40.644602)

    Map("loc_2" -> grid2.convertXYToRowCol(Mx, My).toText, "loc_5" -> grid5.convertXYToRowCol(Mx, My).toText,
      "loc_10" -> grid10.convertXYToRowCol(Mx, My).toText, "loc_25" -> grid25.convertXYToRowCol(Mx, My).toText,
      "loc_50" -> grid50.convertXYToRowCol(Mx, My).toText, "loc_100" -> grid100.convertXYToRowCol(Mx, My).toText,
      "loc_200" -> grid200.convertXYToRowCol(Mx, My).toText,
      "loc_500" -> grid500.convertXYToRowCol(Mx, My).toText, "loc_1000" -> grid1000.convertXYToRowCol(Mx, My).toText,
      "loc_2000" -> grid2000.convertXYToRowCol(Mx, My).toText, "loc_4000" -> grid4000.convertXYToRowCol(Mx, My).toText,
      "loc_5000" -> grid5000.convertXYToRowCol(Mx, My).toText, "loc_10000" -> grid10000.convertXYToRowCol(Mx, My).toText,
      "loc_15000" -> grid15000.convertXYToRowCol(Mx, My).toText, "loc_25000" -> grid25000.convertXYToRowCol(Mx, My).toText,
      "loc_50000" -> grid50000.convertXYToRowCol(Mx, My).toText, "loc_75000" -> grid75000.convertXYToRowCol(Mx, My).toText,
      "loc_100000" -> grid100000.convertXYToRowCol(Mx, My).toText, "loc" -> f"$yVal%.6f,$xVal%.6f",
      "loc_mx" -> Mx, "loc_my" -> My, "lat" -> yVal, "lon" -> xVal)
  }

  def isValidNumeric(input: String, dataType: String): Boolean = {
    var isValid: Boolean = false
    dataType.toUpperCase match {
      case "NUMBER" =>
        isValid = Try(input.toInt).isSuccess
      case "FLOAT" =>
        isValid = Try(input.toFloat).isSuccess
      case "DOUBLE" =>
        isValid = Try(input.toDouble).isSuccess
    }
    isValid
  }

  def getFemtoStatus(cellId: Int): Integer = {
    val eNBId: Int = (cellId & 0xFFFFFF00) >> 8
    var femtoStatus: Integer = 0
    if (eNBId >= 1024000 && eNBId <= 1048575) {
      femtoStatus = 1
    }
    femtoStatus
  }

  def getModulationType(input: String): Integer = {
    var modType: Integer = null
    try {
      if (input != null && !input.trim.isEmpty) {
        val modTypeDB: String = input
        if (!modTypeDB.isEmpty) {
          modTypeDB match {
            case "QPSK"   => modType = 2
            case "16QAM" | "16_QAM" => modType = 4
            case "64QAM" | "64_QAM" => modType = 6
            case "256QAM" | "256_QAM" => modType = 8
          }
        }
      }
    } catch {
      case ge: Exception =>
        logger.warn("Unable to flatten Modulation Type : " + ge.getMessage)
    }
    modType
  }

  def getPucchAdjTargetPwr(pucchTxPower: Int,
                           pathLoss: Int,
                           p0_NominalPUCCH: Integer,
                           pucchType: String) = {
    var targetMobPUCCH: Int = 0
    var TxAdjustforPUCCH: Int = 0
    var pucchVal: java.lang.Float = null
    if (p0_NominalPUCCH != null && (p0_NominalPUCCH <= -96) && (pucchTxPower >= -60 && pucchTxPower <= 20)) {
      // Note: Following formulas are custom formula.
      try {
        targetMobPUCCH = pathLoss + p0_NominalPUCCH
        TxAdjustforPUCCH = pucchTxPower - targetMobPUCCH
        if (pucchType.toLowerCase == "adjtxpwr")
          pucchVal = TxAdjustforPUCCH.toFloat
        else
          pucchVal = targetMobPUCCH.toFloat
      } catch {
        case ge: Throwable =>
          logger.error(
            "Exception occured while calulating " + pucchType + ". Details : " + ge.getMessage)
      }
    }
    pucchVal
  }

  def getServingCellPCI(servingCellIndex_B193: String,
                        PhysicalCellID_b193: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var PCI: Array[(String, Int)] = null
    if (!servingCells.isEmpty && !PhysicalCellID_b193.isEmpty) {
      try {
        val PhysicalCellID: List[Int] =
          PhysicalCellID_b193.split(",").map(_.trim).toList.map(_.toInt)
        PCI = ((servingCells zip PhysicalCellID).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + PhysicalCellID_b193 + ". Details : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getServingCellPCI for Index : " + PhysicalCellID_b193 + " Message : " + ge.getMessage)
          println(
            "ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getServingCellPCI Cause: " + ge
              .getCause())
          logger.error(
            "ES Exception occured while executing getServingCellPCI StackTrace: " + ge
              .printStackTrace())
      }
    }
    PCI
  }

  def getNrCarrierMcs(carrierId: List[String], NrMcs: List[String]) = {
    var servingCells: List[String] = null

    var Mcs: Array[(String, String)] = null
    if (carrierId != null && NrMcs != null) {
      try {
        Mcs = ((carrierId zip NrMcs).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + PhysicalCellID_b193 + ". Details : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrCarrierMcs for carrierId : " + carrierId + " Message : " + ge.getMessage)
          println("ES Exception occured while executing getNrCarrierMcs : " + ge.getMessage)
          logger.error("ES Exception occured while executing getNrCarrierMcs Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing getNrCarrierMcs StackTrace: " + ge.printStackTrace())
      }
    }
    Mcs
  }

  def getNrServingCellKPIs(Nrs_ccindex: String, KPIParams: String) = {
    var servingCells: List[String] = null
    if (Nrs_ccindex != null) {
      servingCells =
        Nrs_ccindex.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
    }
    var Kpi: Array[(String, String)] = null
    if (!servingCells.isEmpty && !KPIParams.isEmpty) {
      try {
        val KPI: List[String] =
          KPIParams.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
        Kpi = ((servingCells zip KPI).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + LteKPIParams + ". Details : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getNrServingCell for Index : " + KPIParams + " Message : " + ge.getMessage)
          println(
            "ES Exception occured while executing getNrServingCell : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getNrServingCell Cause: " + ge
              .getCause())
          logger.error(
            "ES Exception occured while executing getNrServingCell StackTrace: " + ge
              .printStackTrace())
      }
    }
    Kpi
  }

  def get0x2501KPIs(pci: List[String], KPIParams: List[String]) = {
    var Kpi: Array[(String, String)] = null
    if (!pci.isEmpty && !KPIParams.isEmpty) {
      try {
        Kpi = ((pci zip KPIParams).toMap).toArray
      } catch {
        case ge: Exception =>
          logger.error("ES Exception occured while executing get0x2501KPIs for Index : " + KPIParams + " Message : " + ge.getMessage)
          println("ES Exception occured while executing get0x2501KPIs : " + ge.getMessage)
          logger.error("ES Exception occured while executing get0x2501KPIs Cause: " + ge.getCause())
          logger.error("ES Exception occured while executing get0x2501KPIs StackTrace: " + ge.printStackTrace())
      }
    }
    Kpi
  }

  def getFreqRsrp0x2157KPIs(freqRsrp0x2157Str: String): List[(Int, Double)] = {
    val freqPowStringList = freqRsrp0x2157Str.split(",").map(_.trim).toList
    freqPowStringList.map(key =>
      (key.split("@")(0).toInt, key.split("@")(1).toDouble))
  }

  def getServingCellCaKPIs(servingCellIndex_B193: String,
                           LteKPIParams: String) = {
    var servingCells: List[String] = null
    if (servingCellIndex_B193 != null) {
      servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
    }
    var LTEKpi: Array[(String, String)] = null
    if (!servingCells.isEmpty && !LteKPIParams.isEmpty) {
      try {
        val LteKPI: List[String] = LteKPIParams.split(",").map(_.trim).toList
        LTEKpi = ((servingCells zip LteKPI).toMap).toArray
      } catch {
        case ge: Exception =>
          //logger.error("Exception occured while determining " + LteKPIParams + ". Details : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getServingCellPCI for Index : " + LteKPIParams + " Message : " + ge.getMessage)
          println(
            "ES Exception occured while executing getServingCellPCI : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing getServingCellPCI Cause: " + ge
              .getCause())
          logger.error(
            "ES Exception occured while executing getServingCellPCI StackTrace: " + ge
              .printStackTrace())
      }
    }
    LTEKpi
  }

  def exponentialConvertor(input: String) = {
    var output: String = input
    if (input.nonEmpty) {
      import java.math.BigDecimal
      output = new BigDecimal(input).toPlainString
    }
    output
  }

  def roundingToDecimal(input: String) = {
    var output: String = input
    if (input.nonEmpty) {
      output = BigDecimal(input).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
    }
    output
  }

  def getMOD4g5gFiles(fileNames: List[String]): String = {
    var sb = new StringBuffer()

    val FileCheck = """(\w+)_mod4g5g_merged_([\w.]+)""".r
    try {
      for (i <- 0 to fileNames.size - 1) {
        if (fileNames(i).toLowerCase().matches(FileCheck.toString())) {
          sb.append(fileNames(i))
          sb.append(";")
        }
      }
    } catch {
      case ge: Throwable =>
        logger.error(
          "Exception occured while checking Mod4g5g files : " + ge.getMessage)
    }
    sb.toString()
  }

  def getStateCountyRegion(Mx: Double,
                           My: Double,
                           spark: SparkSession): Map[String, Any] = {
    val scQuery: String = "SELECT s.stusps, c.countyns, c.name, " +
      "(case when s.id_Country=244 then 'USA' else s.vz_regions end) as country " +
      "FROM dmat_logs.counties AS c JOIN dmat_logs.states s ON c.statefp = s.statefp " +
      "WHERE ST_Intersects(c.boundaryshape, ST_GeomFromText('point(" + Mx + "  " + My + ") ', 3857))"
    val regQuery: String = "SELECT vz_regions FROM dmat_logs.regions " +
      "WHERE ST_Intersects(boundaryshape, ST_GeomFromText('point(" + Mx + "  " + My + ") ', 3857)) and cur_reg = 1"

    val stateCountyDS = spark.sql(scQuery)
    val regionDS = spark.sql(regQuery)
    Map(
      "Country" -> stateCountyDS.head.get(3).toString,
      "County" -> stateCountyDS.head.get(0).toString,
      "StateCode" -> stateCountyDS.head.get(0).toString,
      "VzwRegions" -> regionDS.head.get(0).toString
    )
  }

  def getJsonValueByKey(jsonStr: String, jsonColName: String) = {
    (parse(jsonStr) \\ jsonColName).values.getOrElse(jsonColName, "").toString
  }

  def getPhychanBitMask(phyBitMask: String): String = {
    var PhyBitMask: String = ""
    if (phyBitMask.nonEmpty) {
      if (phyBitMask.indexOf(":") > 0) {
        PhyBitMask = phyBitMask.split(":")(0)
      } else if (phyBitMask.indexOf("_") > 0) {
        PhyBitMask = phyBitMask.split("_")(0)
      } else {
        PhyBitMask = phyBitMask
      }
    }
    PhyBitMask
  }

  def maxDurationMsVal(a: Double,
                       b: Double,
                       c: Double,
                       d: Double,
                       e: Double): java.lang.Double = {
    var MaxDurationMs: java.lang.Double = 0.0
    try {
      var kpis = new ListBuffer[Double]()
      if (a >= 0.0) kpis += a.toDouble
      if (b >= 0.0) kpis += b.toDouble
      if (c >= 0.0) kpis += c.toDouble
      if (d >= 0.0) kpis += d.toDouble
      if (e >= 0.0) kpis += e.toDouble

      MaxDurationMs = kpis.toList.max.asInstanceOf[java.lang.Double]
    } catch {
      case ge: Throwable =>
        logger.error(
          "Exception occured while determining Elapsed_Time: " + ge.getMessage)
    }
    MaxDurationMs
  }

  case class Location(lat: Double, lon: Double)

  class DistanceCalculatorImpl {
    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    private val AVERAGE_RADIUS_OF_EARTH_Miles = 3958.8

    def calculateDistanceInMiles(userLocation: Location,
                                 warehouseLocation: Location): Float = {
      val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
      val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
      val sinLat = Math.sin(latDistance / 2)
      val sinLng = Math.sin(lngDistance / 2)
      val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(userLocation.lat)) *
          Math.cos(Math.toRadians(warehouseLocation.lat)) *
          sinLng * sinLng)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      (AVERAGE_RADIUS_OF_EARTH_Miles * c).toFloat
    }

  }

  def DistanceFrmCellsToLoc(CurrentLat: Double,
                            CurrentLong: Double,
                            PciLat: Double,
                            PciLong: Double): Float = {
    new DistanceCalculatorImpl().calculateDistanceInMiles(
      Location(CurrentLat, CurrentLong),
      Location(PciLat, PciLong))
  }

  import java.math.BigDecimal

  def round(d: java.lang.Double, decimalPlace: Int): java.lang.Double = {
    var bd = new BigDecimal(d.toString)
    bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP)
    bd.doubleValue();
  }

  def getAvgRbUtilization(count: java.lang.Integer, numerology: String): java.lang.Double = {
    var avgRB: java.lang.Double = 0D
    if (numerology == "15kHz") {
      val slotDurationMs = 1.0
      avgRB = (count * slotDurationMs / 1000 * 100)
    } else if (numerology == "30kHz") {
      val slotDurationMs = 0.5
      avgRB = (count * slotDurationMs / 1000 * 100)
    } else if (numerology == "60kHz") {
      val slotDurationMs = 0.25
      avgRB = (count * slotDurationMs / 1000 * 100)
    } else if (numerology == "120kHz") {
      val slotDurationMs = 0.125
      avgRB = (count * slotDurationMs / 1000 * 100)
    }
    avgRB
  }

  def getRtpGapType(timestampInSeconds: Float): java.lang.Integer = {
    var gapType: java.lang.Integer = null
    if (timestampInSeconds >= 0.5 && timestampInSeconds < 1) {
      gapType = 1
    } else if (timestampInSeconds >= 1 && timestampInSeconds < 3) {
      gapType = 2
    } else if (timestampInSeconds >= 3 && timestampInSeconds < 5) {
      gapType = 3
    } else if (timestampInSeconds >= 5 && timestampInSeconds < 10) {
      gapType = 4
    } else if (timestampInSeconds >= 10) {
      gapType = 5
    }
    gapType
  }

  def checkVolteCallFailure(subtitleMsg: String, sipCseq: String, nasMsg: String): java.lang.Integer = {

    var isVolteCallIneffective: java.lang.Integer = 1
    var subtitlePairs: List[String] = null
    var sipCseqPairs: List[String] = null
    var nasMsqList: List[String] = null

    if (subtitleMsg != null) {
      subtitlePairs = subtitleMsg.split(",").map(_.trim).toList
    }
    if (sipCseq != null) {
      sipCseqPairs = sipCseq.split(",").map(_.trim).toList
    }

    var sipSubtitlMsg_sCeq: Array[(String, String)] = null
    if ((!subtitlePairs.isEmpty && !sipCseqPairs.isEmpty) && subtitlePairs.contains("INVITE")) {
      try {
        sipSubtitlMsg_sCeq = ((subtitlePairs zip sipCseqPairs).toMap).toArray

        var subititleandcSeqWithInvite: Boolean = false
        var subititleandcSeqWith_200andInvite: Boolean = false
        var subtitleWith200_ok:Boolean = false
        var NasRequest_Accept: Boolean = false
        var NasRequest: Boolean = false
        var NasAccept: Boolean = false

        for (i <- 0 to sipSubtitlMsg_sCeq.size - 1) {
          sipSubtitlMsg_sCeq(i)._1 match {
            case "INVITE" =>
              if (sipSubtitlMsg_sCeq(i)._2 == "1 INVITE") {
                if (nasMsg != null) {
                  nasMsqList = nasMsg.split(",").map(_.trim).toList
                  if(nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST") && nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT")) {
                    NasRequest_Accept = true
                  } else if (nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST")) {
                    NasRequest = true
                  } else if (nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT")) {
                    NasAccept = true
                  }
                }
                subititleandcSeqWithInvite = true
                if(subititleandcSeqWith_200andInvite && (NasRequest_Accept || NasRequest || NasAccept)) {
                  isVolteCallIneffective= 0
                } else if(subititleandcSeqWithInvite && NasRequest_Accept){
                  isVolteCallIneffective = 0
                } else if(subititleandcSeqWith_200andInvite) {
                  isVolteCallIneffective= 0
                }

              }
            case "200 OK" =>
              if (sipSubtitlMsg_sCeq(i)._2 == "1 INVITE") {
                if (nasMsg != null) {
                  nasMsqList = nasMsg.split(",").map(_.trim).toList
                  if(nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST") && nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT")) {
                    NasRequest_Accept = true
                  } else if (nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST")) {
                    NasRequest = true
                  } else if (nasMsqList.contains("ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT")) {
                    NasAccept = true
                  }
                }
                subititleandcSeqWith_200andInvite = true
                subtitleWith200_ok = true
                if(subititleandcSeqWithInvite && (NasRequest_Accept || NasRequest || NasAccept)) {
                  isVolteCallIneffective = 0
                } else if(subititleandcSeqWith_200andInvite) {
                  isVolteCallIneffective= 0
                } else if (subtitleWith200_ok && NasRequest) {
                  isVolteCallIneffective= 0
                }

              }
            case _ =>
              if(subititleandcSeqWithInvite && NasRequest_Accept){
                isVolteCallIneffective = 0
              }  else if(subititleandcSeqWithInvite && NasRequest) {
              isVolteCallIneffective = 1
              } else {
                isVolteCallIneffective= 1
              }
          }
        }
      } catch {
        case ge: Exception =>
          logger.error(
            "ES Exception occured while executing checkVolteCallFailure for seq : " + sipCseq + " Message : " + ge.getMessage)
          println(
            "ES Exception occured while executing checkVolteCallFailure : " + ge.getMessage)
          logger.error(
            "ES Exception occured while executing checkVolteCallFailure Cause: " + ge
              .getCause())
          logger.error(
            "ES Exception occured while executing checkVolteCallFailure StackTrace: " + ge
              .printStackTrace())
      }
    } else {
      isVolteCallIneffective = 0
    }
    isVolteCallIneffective
  }

  def calcilateLacIdAndPlmnId(Gsm_Lai: List[Byte]) = {
    var lacId: Int = 0
    var plmnId: String = ""
    if (Gsm_Lai != null) {
      //logger.info("0x5B34 Process Dmat Gsm_Lai: "+ Gsm_Lai.size);
      //logger.info("0x5B34 Process Dmat Gsm_Lai: "+ Gsm_Lai);
      if (Gsm_Lai.size == 5) {
        var mcc: String = null
        var mnc: String = null
        var lac: String = null
        val ByteInHex0 = ParseUtils.byteToHex(Gsm_Lai(0))
        val ByteInHex1 = ParseUtils.byteToHex(Gsm_Lai(1))
        val ByteInHex2 = ParseUtils.byteToHex(Gsm_Lai(2))
        val ByteInHex3 = ParseUtils.byteToHex(Gsm_Lai(3))
        val ByteInHex4 = ParseUtils.byteToHex(Gsm_Lai(4))

        mcc = ByteInHex0.substring(1, 2) + ByteInHex0.substring(0, 1) + ByteInHex1.substring(1, 2)
        mnc = ByteInHex2.substring(1, 2) + ByteInHex2.substring(0, 1) + ByteInHex1.substring(0, 1)
        lac = ByteInHex3 + ByteInHex4
        val lacStr = "0x" + lac
        lacId = ParseUtils.intFromBytes(Gsm_Lai(3), Gsm_Lai(4))

        plmnId = "MCC/MNC: " + mcc + "/" + mnc
      }
    }
    (lacId, plmnId)
  }

}
