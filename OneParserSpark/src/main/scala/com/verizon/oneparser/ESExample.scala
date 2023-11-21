package com.verizon.oneparser

import java.net.SocketException
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.{KafkaUpdate, Producer}
import com.verizon.oneparser.common.{Constants, SecurityUtil}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.LogsData
import com.verizon.oneparser.eslogrecord._
import org.apache.spark.sql._

import scala.collection.immutable._
import scala.collection.mutable

//import org.elasticsearch.script.ScriptType
//import org.elasticsearch.script.mustache.{SearchTemplateRequest, SearchTemplateRequestBuilder, SearchTemplateResponse}
object ESExample extends Serializable with LazyLogging {
  private val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
  private val conf = spark.sparkContext.getConf
  conf.setAppName("DMATESDataIngestionProcess")
  conf.set("mapreduce.input.fileinputformat.split.maxsize", "10000000")


  def main(args: Array[String]): Unit = {
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    val currentDate: String = dateFormatter.format(new Date())
    var logsTblDs: DataFrame = null
    val startTimeRead = System.currentTimeMillis
    logger.info("Started reading file ==>> " + startTimeRead)
    val limit = spark.conf.get("spark.esLimit")
    val sleepTime = spark.conf.get("spark.esSleepTime")
    val timeInterval = spark.conf.get("spark.esTimeInterval")
    val repartitionNo = spark.conf.get("spark.repartition")
    val writeToHdfsRepartitionNo = spark.conf.get("spark.writeToHdfsRepartition")
    val rangePartitionNo = spark.conf.get("spark.rangeRepartitionSize")
    val writeFilePayloadToHdfs = spark.conf.get("spark.PROCESSDMAT_HDFS_WRITE")
    val commonConfigParams: CommonConfigParameters = CommonConfigParameters(CURRENT_ENVIRONMENT = spark.conf.get("spark.CURRENT_ENVIRONMENT"), POSTGRES_CONN_URL = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_CONN_URL")), POSTGRES_CONN_SDE_URL = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_CONN_SDE_URL")),
      POSTGRES_DRIVER = spark.conf.get("spark.POSTGRES_DRIVER"), POSTGRES_DB_URL = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_DB_URL")), POSTGRES_DB_SDE_URL = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_DB_SDE_URL")),
      POSTGRES_DB_USER = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_DB_USER")), POSTGRES_DB_PWD = SecurityUtil.decrypt(spark.conf.get("spark.POSTGRES_DB_PWD")), HIVE_HDFS_PATH = spark.conf.get("spark.HIVE_HDFS_PATH"),
      HDFS_FILE_DIR_PATH = spark.conf.get("spark.HDFS_FILE_DIR_PATH"), HDFS_URI = spark.conf.get("spark.HDFS_URI"), WRITE_HDFS_URI = spark.conf.get("spark.WRITE_HDFS_URI"), ES_HOST_PATH = spark.conf.get("spark.ES_HOST_PATH"), ES_PORT_NUM = spark.conf.get("spark.ES_PORT_NUM").toInt, DM_USER_DUMMY = spark.conf.get("spark.DM_USER_DUMMY").toInt,
      HIVE_DB = spark.conf.get("spark.HIVE_DB"), TBL_QCOMM_NAME = spark.conf.get("spark.TBL_QCOMM_NAME"), TBL_QCOMM2_NAME = spark.conf.get("spark.TBL_QCOMM2_NAME"), TBL_B192_NAME = spark.conf.get("spark.TBL_B192_NAME"),
      TBL_B193_NAME = spark.conf.get("spark.TBL_B193_NAME"), TBL_ASN_NAME = spark.conf.get("spark.TBL_ASN_NAME"), TBL_NAS_NAME = spark.conf.get("spark.TBL_NAS_NAME"), TBL_IP_NAME = spark.conf.get("spark.TBL_IP_NAME"), TBL_QCOMM5G_NAME = spark.conf.get("spark.TBL_QCOMM5G_NAME"),
      SEQ_FILE_DEL = spark.conf.get("spark.SEQ_FILE_DEL"), DETAILED_HIVE_JOB_ANALYSIS = spark.conf.get("spark.DETAILED_HIVE_JOB_ANALYSIS"), REPORTS_HIVE_TBL_NAME = spark.conf.get("spark.REPORTS_HIVE_TBL_NAME"), PROCESSING_SERVER = spark.conf.get("spark.PROCESSING_SERVER"), PROCESS_RM_ONLY = spark.conf.get("spark.PROCESS_RM_ONLY"),
      ES_CLUSTER_NAME = spark.conf.get("spark.es_cluster_name"), ES_NODE_ID = spark.conf.get("spark.es_node_id"), ES_PORT_ID = spark.conf.get("spark.es_port_id"), ES_CELLSITE_INDEX = spark.conf.get("spark.es_cellsite_index"), ES_CELLSITE_WEEKLY_INDEX_DATE = spark.conf.get("spark.cellsite_weekly_index_date"),
      BOOTSTRAP_SERVERS = spark.conf.get("spark.BOOTSTRAP_SERVERS"))

    val producer = new Producer(spark, commonConfigParams)
    val kafkaUpdate = new KafkaUpdate(producer)

    import org.apache.spark.sql.functions._
    val kafkaOptions = mutable.Map[String, String]() //TODO
    kafkaOptions.put("kafka.bootstrap.servers", commonConfigParams.BOOTSTRAP_SERVERS)
    kafkaOptions.put("topic", "esTopic")
    val renameDlf: String => String = (data: String) => {
      Constants.getSeqFromDlf(data)
    }
    val renameCol = udf(renameDlf)
    val df = spark.readStream
      .format("kafka").options(kafkaOptions).load()

    df
      .withColumn("fileName", renameCol(df.col("fileName")))
      .foreach(row => {
        process(logsTblDs, rangePartitionNo, commonConfigParams, row, kafkaUpdate)
      })
  }

  private def process(logsTblDs: DataFrame, rangePartitionNo: String, commonConfigParams: CommonConfigParameters, row: Row, kafkaUpdate: KafkaUpdate) = {
    try {
      val filename = row(row.fieldIndex("fileName")).toString
      val date = Constants.getDateFromFileLocation(row(row.fieldIndex("fileLocation")).toString)
      val testId = row(row.fieldIndex("testId")).toString
      val datesStr: String = "'" + date + "','" + Constants.fetchUpdatedDirDate + "'"
      val testIdStr = "'" + testId + "'"
      val fileNameStr = "'" + filename + "'"

      kafkaUpdate.updateEsStatusInLogs(logsTblDs, Constants.ES_INPROGRESS)

      logger.info("Get the filenames: " + fileNameStr)
      val dmatQualCommQuery: String = {
        "select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + QualCommQuery.getQCommQuery(false) +
          "from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          "from dmat_logs." + commonConfigParams.TBL_QCOMM_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNameStr + ") and testId in (" + testIdStr + "))) where rn = 1 "
      }

      import spark.implicits._
      val dmatQualComm1 = spark.sql(dmatQualCommQuery).repartitionByRange(rangePartitionNo.toInt, $"filename")
      dmatQualComm1.createOrReplaceTempView("dmatQualComm1")

      val dmatQualComm2 = {
        spark.sql("select * from (select fileName,dmTimeStamp,dateWithoutMillis, " + QualComm2Query.getQComm2Query(false) +
          " from (select *, split(dmTimeStamp, '[\\.]')[0] as dateWithoutMillis " +
          " from dmat_logs." + commonConfigParams.TBL_QCOMM2_NAME + " where insertedDate in (" + datesStr + ") and filename in (" + fileNameStr + ") and testId in (" + testIdStr + ") and (inbldUserMark != 'true' or inbldUserMark != 'false'))) where rn = 1").repartitionByRange(rangePartitionNo.toInt, $"filename")
      }

      var joinedAllTablesTemp: DataFrame = null

      if (dmatQualComm1.count() > 0) {
        joinedAllTablesTemp = dmatQualComm1.join(dmatQualComm2, Seq("fileName", "dateWithoutMillis"), "full_outer").repartitionByRange(rangePartitionNo.toInt, $"filename")
      }

      if (joinedAllTablesTemp.count() > 0) {
        logger.info("end of join operationf>>>>>")
      }
    } catch {
      case e: SocketException =>
        logger.error("Socket exception occured while processing files : " + e.getMessage)
        logger.error("Socket exception occured while processing files : " + e.printStackTrace())
        if (logsTblDs != null) {
          kafkaUpdate.updateEsStatusInLogs(logsTblDs, Constants.HIVE_SUCCESS)
        }
        spark.stop()

      case e: Exception =>
        logger.error("Main Catch : ES Exception occured while executing main block into ES : " + e.getMessage)
        println("Main Catch : ES Exception occured while executing main block into ES : " + e.getMessage)
        logger.error("Main Catch : ES Exception occured while executing main block into ES : " + e.printStackTrace())
    }
  }
}

