package com.verizon.oneparser.process

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.{Constants, HiveTableStructure, OPutil}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.{Logs, ProcessParamValues}
import com.verizon.oneparser.parselogs.{LogRecordParser5G, LogRecordParserASN, LogRecordParserB192, LogRecordParserB193, LogRecordParserIP, LogRecordParserNAS, LogRecordParserQComm, LogRecordParserQComm2, LogRecordParserSIG}
import com.verizon.oneparser.schema.LogRecord
import com.vzwdt.parseutils.ParseUtils
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.sql.functions.{col, input_file_name, split, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Try

case object ReadSequenceToHiveProc extends LazyLogging {

  def unit(spark: SparkSession, config: CommonConfigParameters, ppv: ProcessParamValues = null, sourceDF: DataFrame = null): ReadSequenceToHiveProc = {
    ReadSequenceToHiveProc(spark, config, ppv, sourceDF)
  }
}

case class ReadSequenceToHiveProc(spark: SparkSession, config: CommonConfigParameters, ppv: ProcessParamValues, df: DataFrame) extends LazyLogging {

  import spark.implicits._

  /**
   * Returns current DataFrame
   *
   * @return current DataFrame
   */
  def getDataFrame = df

  /**
   * Updates log status to HIVE_INPROGRESS
   *
   * @return current DataFrame
   */
  def preprocess: DataFrame = {
    preprocess(df)
  }

  /**
   * Updates log status to HIVE_INPROGRESS in specified DataFrame
   *
   * @param df specified DataFrame
   * @return current DataFrame
   */
  def preprocess(df: DataFrame): DataFrame = {
    df.as[Logs].mapPartitions(partition => {
      partition.map(log => {
        log.copy(status = Constants.HIVE_INPROGRESS, hiveStartTime = System.currentTimeMillis())
      })
    }).toDF
  }

  /**
   * Starts processing based on current DataFrame
   *
   * @return current DataFrame
   */
  def process: DataFrame = {
    val logRecordDF = df.as[Logs].mapPartitions(partition => {
      partition.map(logs => {
        val get_last = udf((xs: Seq[String]) => Try(xs.last).toOption)

        val logsUpdatedTime =
          if (logs.updatedTime == null)
            Constants.getDateFromFileLocation(logs.fileLocation)
          else
            new SimpleDateFormat("MMddyyyy").format(new Date(logs.updatedTime))

        val seqfilePath = Constants.getSeqFilePathFromDlf(logs.fileName, logs.fileLocation, logsUpdatedTime, config.HDFS_FILE_DIR_PATH)

        val seqfile = spark.sparkContext.sequenceFile[IntWritable, BytesWritable](seqfilePath, 1)
          .map(x => (x._1.get(), x._2.copyBytes()))
          .toDF()
          .withColumn("fileName", get_last(split(input_file_name(), "/")))
          .filter($"fileName" === seqfilePath)
          .first()
        val seqfileKey = seqfile.getAs[Integer]("_1")
        val seqfileRecord = seqfile.getAs[mutable.WrappedArray[Byte]]("_2").array
        val logRecord = parseDMATLogCodes(logs, seqfileKey, seqfileRecord)
        logRecord
      })
    }).toDF

    logRecordDF
  }

  private def parseDMATLogCodes(log: Logs, seqfileKey: Integer, seqfileRecord: Array[Byte]) = {
    val logcodes_qcomm_set1 = Array(45433, 45450, 45389, 45390, 45249, 28981)
    val logcodes_qcomm_set2 = Array(45250, 45369)
    val logcodes_qcomm_set3 = Array(45416, 45426)
    val logcodes_qcomm_set4 = Array(4119, 4201, 4238, 4506, 1498, 5435, 16679, 20680, 20780, 20784, 20788, 23240, 23340, 23344, 23348, 65521)
    val logcodes_qcomm_set5 = Array(45422, 45423, 65535,5238,4116)
    var logcodes_qcomm_set6 = Array(45425,45427)
    var logcodes_qcomm_set7 = Array(45362, 45350)

    var logcodes_qcomm2_set1 = Array(65529, 65530, 65531, 8187, 4943, 6222, 45207, 45155, 45156, 65525)
    var logcodes_qcomm2_set2 = Array(45294, 45191, 45411, 45220,45409, 45236)

    val logcodes_qcomm5g_set1 = Array(47138, 47170, 47181, 47477)
    val logcodes_qcomm5g_set2 = Array(47240, 47241, 47236)
    val logcodes_qcomm5g_set3 = Array(47239)

    val logcodes_asn_set = Array(45248, 47137)
    val logcodes_b193_set = Array(45459)
    val logcodes_b192_set = Array(45458)

    val logcodes_nas_set = Array(45282,45283,45292,45293)

    val logcodes_ip_set = Array(65520)

    val logcodes_sig_set = Array(8449,9227,9222,9224,9229,9473)
    val device_types_set = Array(2,8,9,10,15,16,17,18,20,21,23,25,31,33,34,36,37,40,41)

    var currentLogCode = 0
    var currentFileName = log.fileName
    var logFileTimestamp:Timestamp =  null
    var timeStamp:Timestamp = null
    var logRecord: LogRecord = null

    var logCodeVersion = 0
    var currentVersion = 0
    var exceptionUseArray: List[String] = List(currentFileName, currentLogCode.toString, currentVersion.toString)

    if (currentFileName.contains("_sig.seq")) {
      val sigSeqRecord = seqfileRecord
      val deviceType = ParseUtils.intFromByte(sigSeqRecord(4))
      if (device_types_set contains deviceType) {
        val sigSeqRecMod = OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(seqfileRecord)
        currentLogCode = ParseUtils.intFromBytes(sigSeqRecMod(2), sigSeqRecMod(3))
        if (logcodes_sig_set contains currentLogCode) {
          logFileTimestamp = Constants.toUTCTimeStamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod))
          logCodeVersion = if (deviceType == 41) ParseUtils.intFromByte(sigSeqRecMod(14)) else ParseUtils.intFromByte(sigSeqRecMod(12))
        } else {
          logFileTimestamp = null
          logCodeVersion = 0
        }
      } else {
        currentLogCode = ParseUtils.intFromBytes(seqfileRecord(2), seqfileRecord(3))
        logFileTimestamp = Constants.toUTCTimeStamp(ParseUtils.parseTimestamp(seqfileRecord).getTime)
      }

      timeStamp = if(logFileTimestamp!=null && Constants.isValidDate(logFileTimestamp, currentLogCode, currentFileName)) logFileTimestamp else null

      logRecord = LogRecord(currentFileName,
        timeStamp, seqfileKey,
        currentLogCode.toString,
        "",
        logCodeVersion, Constants.fetchUpdatedDirDate(), log.id.asInstanceOf[Int])
    } else {
      currentLogCode = ParseUtils.intFromBytes(seqfileRecord(3), seqfileRecord(2))
      logFileTimestamp = Constants.toUTCTimeStamp(ParseUtils.parseTimestamp(seqfileRecord).getTime)
      timeStamp = if(Constants.isValidDate(ParseUtils.parseTimestamp(seqfileRecord), currentLogCode, currentFileName)) logFileTimestamp else null
      logRecord = LogRecord(log.fileName,
        timeStamp, seqfileKey,
        "",
        "",
        ParseUtils.intFromByte(seqfileRecord(12)), Constants.fetchUpdatedDirDate(), log.id.asInstanceOf[Int])
      logCodeVersion = ParseUtils.intFromByte(seqfileRecord(12))
      currentVersion = ParseUtils.intFromByte(seqfileRecord(12))
    }

    if (timeStamp != null) {
      try {
        if (logcodes_asn_set contains currentLogCode) {
          logRecord = LogRecordParserASN.parseLogRecordASNSet1(currentLogCode,logRecord,logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm2_set1 contains currentLogCode) {
          logRecord = LogRecordParserQComm2.parseLogRecordQCOMM2_Set2(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm2_set1 contains currentLogCode) {
          logRecord = LogRecordParserQComm2.parseLogRecordQCOMM2_Set3(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm5g_set1 contains currentLogCode) {
          logCodeVersion = ParseUtils.intFrom4Bytes(seqfileRecord(15),seqfileRecord(14),seqfileRecord(13),seqfileRecord(12))
          logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet1(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm5g_set2 contains currentLogCode) {
          logCodeVersion = ParseUtils.intFrom4Bytes(seqfileRecord(15),seqfileRecord(14),seqfileRecord(13),seqfileRecord(12))
          logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet2(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm5g_set3 contains currentLogCode) {
          logCodeVersion = ParseUtils.intFrom4Bytes(seqfileRecord(15),seqfileRecord(14),seqfileRecord(13),seqfileRecord(12))
          logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet3(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_b193_set contains currentLogCode) {
          logRecord = LogRecordParserB193.parseLogRecordB193(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_b192_set contains currentLogCode) {
          logRecord = LogRecordParserB192.parseLogRecordB192(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set1 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set1(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set2 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set2(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set3 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set3(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set4 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set4(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set5 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set5(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set6 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set6(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_qcomm_set7 contains currentLogCode) {
          logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set7(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_nas_set contains currentLogCode){
          logRecord = LogRecordParserNAS.parseLogRecordNASSet1(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if(logcodes_ip_set contains currentLogCode){
          logRecord = LogRecordParserIP.parseLogRecordIPSet1(currentLogCode, logRecord, logCodeVersion,seqfileRecord, exceptionUseArray)
        } else if (logcodes_sig_set contains currentLogCode){
          logRecord = LogRecordParserSIG.parseLogRecordSIGSet1(currentLogCode,logRecord,logCodeVersion,seqfileRecord,exceptionUseArray)
        }
      }
      catch {
        case npe: NullPointerException => {
          logger.error("Null Pointer Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, npe.getMessage)
          logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
            exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Null Pointer Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + npe.getMessage)
        }
        case aiob: ArrayIndexOutOfBoundsException => {
          logger.error("Array Index Out of bound Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, aiob.getMessage)
          logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
            exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Array Index Out of bound Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + aiob.getMessage)
        }
        case ae: ArithmeticException => {
          logger.error("Arithmetic Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, ae.getMessage)
          logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
            exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Arithmetic Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + ae.getMessage)
        }
        case cce: ClassCastException => {
          logger.error("Class Cast Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, cce.getMessage)
          logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
            exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Class Cast Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + cce.getMessage)
        }
        case ge: Exception => {
          if (ge.getMessage != null && ge.getMessage == "Internal Parse Exception occured.") {
            logger.error("Internal Parse Exception occured while processing file:{},{},{}.Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, ge.getMessage)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Internal Parse Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + ge.getMessage)
          } else if (config.LOG_GENERAL_EXCEPTION.equalsIgnoreCase("true")) {
            logger.error("General Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, currentVersion.toString, ge.getMessage)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive General Exception for " + currentFileName + "," + currentLogCode.toString + "," + currentVersion.toString + " Details : " + ge.getMessage)
          }
          logger.error("StackTrace for General Exception:{}",ge.getStackTrace)
        }
      }
    }
    logRecord
  }

  def transformHiveStructure(batchDF: DataFrame) = {
    val data2Hive = batchDF.select(flattenSchema(batchDF.schema): _*).filter($"hexLogCode" =!= "")

    var hiveSaveMode = SaveMode.Ignore

    if (ppv.hiveStructChng == "Y" && ppv.hiveCntChk == "Y") {
      if (!checkTableExists(spark, config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + ppv.currentWeek)) {
        spark.sql(HiveTableStructure.generateHiveSchema("HIVE", config.HIVE_DB, config.FINAL_HIVE_TBL_NAME + ppv.currentWeek, config.HDFS_URI))
        hiveSaveMode = SaveMode.Overwrite
      }
    } else {
      if (ppv.hiveStructChng == "Y" && ppv.hiveCntChk == "N") {
        val dataExists = checkTableDataExists(spark, config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + ppv.currentWeek)
        hiveSaveMode = if (!dataExists) SaveMode.Overwrite else SaveMode.Append
      } else {
        if (ppv.hiveCntChk == "N") {
          hiveSaveMode = SaveMode.Append
        }
      }
    }

    if (hiveSaveMode != SaveMode.Ignore) {
      data2Hive.filter($"missingVersion" === -1)
        .write
        .partitionBy("fileName", "insertedDate", "testId")
        .mode(hiveSaveMode)
        .option("path", config.HIVE_HDFS_PATH + config.FINAL_HIVE_TBL_NAME + ppv.currentWeek)
        .saveAsTable(config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + ppv.currentWeek)
    }

    val data2Logs = data2Hive.map(row => {
      val logs = Logs(
        id = row.getAs[Int]("testId").longValue(),
        fileName = row.getAs[String]("fileName"),
        mdn = row.getAs[String]("mdn"),
        imei = row.getAs[String]("imei_QComm2"),
        emailid = row.getAs[String]("email"),
        modelName = row.getAs[String]("modelName"),
        isinbuilding = if ("true".equalsIgnoreCase(row.getAs[String]("isInBuilding"))) 1 else 0,
        technology = if (row.getAs[Boolean]("is5GTechnology")) "5g" else "4g",
        hasgps = row.getAs[Integer]("hasGps"),
        missingVersions = row.getAs[Integer]("missingVersion").toString,
        failureInfo = row.getAs[String]("exceptionCause"),
        status = Constants.HIVE_SUCCESS,
        hiveEndTime = System.currentTimeMillis(),
        reportsHiveTblName = ppv.currentWeek)
      logs
    }).toDF
    data2Logs
  }

  private def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

  private def checkTableExists(spark: SparkSession, tableName: String) = {
    spark.catalog.tableExists(tableName)
  }

  private def checkTableDataExists(spark:SparkSession, tableName:String)={
    val countDS = spark.sql("select count(*) as tableCount from "+tableName).select("tableCount").collectAsList()
    val tableCountAsString = countDS.get(0).get(0).toString
    tableCountAsString!=null && tableCountAsString!="0"
  }
}
