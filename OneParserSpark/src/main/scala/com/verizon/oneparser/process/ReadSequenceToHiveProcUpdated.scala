package com.verizon.oneparser.process

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.broker.KafkaSink
import com.verizon.oneparser.common.{CommonDaoImpl, Constants, HiveTableStructure, OPutil, ProcessParams}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.{Logs, ProcessParamValues}
import com.verizon.oneparser.parselogs._
import com.verizon.oneparser.schema.LogRecord
import com.verizon.oneparser.utils.RunStatistics
import com.vzwdt.parseutils.ParseUtils
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.sql.functions.{col, input_file_name, lit, split, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession, functions}
import com.verizon.oneparser.utils.dataframe.ExtraDataFrameOperations.implicits._
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

import scala.util.Try

case object ReadSequenceToHiveProcUpdated extends LazyLogging {

  def unit(spark: SparkSession, config: CommonConfigParameters, ppv: ProcessParamValues = null, sourceDF: DataFrame = null): ReadSequenceToHiveProcUpdated = {
    ReadSequenceToHiveProcUpdated(spark, config, ppv, sourceDF)
  }
}

case class ReadSequenceToHiveProcUpdated(spark: SparkSession, config: CommonConfigParameters, ppv: ProcessParamValues, df: DataFrame) extends LazyLogging {

  import spark.implicits._

  val kafkaSink: KafkaSink = KafkaSink(spark, config)

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
        var logUpdated = log.copy(status = Constants.HIVE_INPROGRESS, hiveStartTime = System.currentTimeMillis())
        if (config.ODLV_TRIGGER) logUpdated = logUpdated.copy(odlvOnpStatus = Constants.HIVE_INPROGRESS)
        logUpdated
      })
    }).toDF
  }

  def process: Array[String] = {
    if (df == null || df.isEmpty) {
      logger.warn("Input df is null or empty")
      Array[String]()
    } else
      process(df)
  }

  def process(df: DataFrame): Array[String] = {

    kafkaSink.writeBatch(df.withColumn("status", lit(Constants.HIVE_INPROGRESS)).withColumn("hiveStartTime", lit(System.currentTimeMillis())).setNullableStateExcept("id"), config.NOTIFICATION_KAFKA_TOPIC)

    val logRecordSeqFilePaths = df.as[Logs].mapPartitions(partition => {
      partition.map(logs => {
        val logsUpdatedTime =
          if (logs.updatedTime == null)
            Constants.getDateFromFileLocation(logs.fileLocation)
          else
            new SimpleDateFormat("MMddyyyy").format(new Date(logs.updatedTime))

        val seqFileName = if (logs.fileName.endsWith("seq"))
          Constants.getHDFSDirPath("SEQ", Constants.getDateFromFileLocation(logs.fileLocation), logsUpdatedTime, config.HDFS_FILE_DIR_PATH) + logs.fileName
        else
          Constants.getSeqFilePathFromDlf(logs.fileName, logs.fileLocation, logsUpdatedTime, config.HDFS_FILE_DIR_PATH)
        seqFileName
      })
    })

    val seqFiles = logRecordSeqFilePaths.collect().mkString(",")

    val get_last = udf((xs: Seq[String]) => Try(xs.last).toOption)
    val seqFilesDF = spark.sparkContext.sequenceFile[IntWritable, BytesWritable](seqFiles, config.HIVE_MIN_PARTITION)
      .map(x => (x._1.get(), x._2.copyBytes()))
      .toDF()
      .withColumn("fileName", get_last(split(input_file_name(), "/")))

    val currentDate = Constants.fetchUpdatedDirDate()
    val startTime = System.currentTimeMillis
    RunStatistics.time(parseDMATLogCodes(seqFilesDF, currentDate, spark, startTime, df, ppv), s"Processed: $seqFiles")
    extractFileNames(seqFiles)
  }

  def parseDMATLogCodes(input: DataFrame, currentDate: String, spark: SparkSession, startTime: Long, logsTblDs: Dataset[Row], paramsConfig: ProcessParamValues): DataFrame = {
    var resDf = logsTblDs.select("*")

    val idDs = logsTblDs.select($"fileName", $"id")
    val modifiedInput = input.join(idDs, Seq("fileName"), "left_outer").select("_1", "_2", "fileName", "id")

    val dlfList = modifiedInput.rdd.map(x => {
      val logcodes_qcomm_set1 = Array(45433, 45450, 45249, 28981)
      val logcodes_qcomm_set2 = Array(45250, 45369)
      val logcodes_qcomm_set3 = Array(45416, 45426)
      val logcodes_qcomm_set4 = Array(4119, 4201, 4238, 4506, 1498, 5435, 16679, 20680, 20780, 20784, 20788, 23240, 23340, 23344, 23348, 65521)
      val logcodes_qcomm_set5 = Array(45422, 45423, 65535, 5238, 4116)
      val logcodes_qcomm_set6 = Array(45425, 45427)
      var logcodes_qcomm_set7 = Array(45362, 45350)
      var logcodes_qcomm_set8 = Array(61668, 45390, 45389)

      var logcodes_qcomm2_set1 = Array(65529, 65530, 65531, 4943, 6222, 45207, 45155, 45156, 65523, 65525)
      val logcodes_qcomm2_set2 = Array(45294, 45191, 45411, 45220, 45409, 45236)
      val logcodes_qcomm2_set3 = Array(8187)

      val logcodes_qcomm5g_set1 = Array(47138, 47170, 47181, 47247, 47249, 47271, 47477)
      val logcodes_qcomm5g_set2 = Array(47240, 47241, 47314, 47320, 47325, 47242, 47525)
      val logcodes_qcomm5g_set3 = Array(47239)
      val logcodes_qcomm5g_set6 = Array(47235, 47233, 47208, 47200)
      val logcodes_qcomm5g_set7 = Array(47506, 47248)
      val logcodes_qcomm5g_set5 = Array(47487, 47141)

      val logcodes_asn_set = Array(45248, 47137)
      val logcodes_b193_set = Array(45459)
      val logcodes_b192_set = Array(45458)

      val logcodes_nas_set = Array(45282, 45283, 45292, 45293)

      val logcodes_ip_set = Array(65520)

      val logcodes_sig_set = Array(8449, 9227, 9222, 9224, 9229, 9473, 8535) //,9228)
      val logcodes_trace_set  = Array(65524)
      val device_types_set = Array(2, 8, 9, 10, 15, 16, 17, 18, 20, 21, 23, 25, 30, 31, 33, 34, 36, 37, 40, 41)
      var sequencePayload = x.get(1).asInstanceOf[Array[Byte]]
      var currentLogCode = 0
      var currentFileName = x.getString(x.fieldIndex("fileName"))
      var logFileTimestamp: Timestamp = null
      var timeStamp: Timestamp = null
      var logRecord: LogRecord = null

      var logCodeVersion = 0
      val isSigFile = if (currentFileName.contains("_sig.seq")) true else false;
      var sigSeqRecMod = sequencePayload

      if (currentFileName.contains("_sig.seq")) {
        val sigSeqRecord = sequencePayload
        val deviceType = ParseUtils.intFromByte(sigSeqRecord(4))
        if (device_types_set contains deviceType) {
          sigSeqRecMod = OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(sequencePayload)
          currentLogCode = ParseUtils.intFromBytes(sigSeqRecMod(2), sigSeqRecMod(3))
          if (logcodes_sig_set contains currentLogCode) {
            logFileTimestamp = Constants.toUTCTimeStamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod))
            /*logger.info("For LogFileTimeStamp>>>>>>>>>>"+logFileTimestamp+"   >>Whole PayLoad Before Modifying................."+OPutil.printHexBinary(sigSeqRecord,true) +
              "PayLoad After Modifying(DropHeader)................."+OPutil.printHexBinary(sigSeqRecMod,true))
            logger.info("For LogFileTimeStamp>>>>>>>>>>"+logFileTimestamp+" Finding Out Timestamp as per LV.................."+new java.sql.Timestamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod)) + " with EPOCHTIME: "+
              new java.sql.Timestamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod)).getTime +
              ">>>>>>>>>>"+"Finding Out Timestamp as per OP.................."+logFileTimestamp + " with EPOCHTIME: "+
              logFileTimestamp.getTime )*/
            logCodeVersion = if (deviceType == 41) ParseUtils.intFromByte(sigSeqRecMod(14)) else ParseUtils.intFromByte(sigSeqRecMod(12))
            //logger.info("Current LogCode........"+currentLogCode)
          }
          else {
            logFileTimestamp = null
            logCodeVersion = 0
          }
        }
        else if (deviceType == 26 || deviceType == 42) {
          sigSeqRecMod = OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(sequencePayload)
          logCodeVersion = ParseUtils.intFromByte(sigSeqRecMod(12))
          currentLogCode = ParseUtils.intFromBytes(sigSeqRecMod(3), sigSeqRecMod(2))
          logFileTimestamp = Constants.toUTCTimeStamp(ParseUtils.parseTimestamp(sigSeqRecMod).getTime) //Constants.toUTCTimeStamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod))
          /*logger.info("Current Log Code.... "+currentLogCode+ " with Hex String: "+currentLogCode.toHexString.toUpperCase+" with Version : "+logCodeVersion
            +" LogFile TimeStamp : "+logFileTimestamp + " Without Applying UTC...."+ParseUtils.parseTimestamp(sigSeqRecMod) + " HbFlex TimeStamp : "+Constants.toUTCTimeStamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod)))*/
        }
        else {
          currentLogCode = ParseUtils.intFromBytes(sequencePayload(2), sequencePayload(3))
          logFileTimestamp = Constants.toUTCTimeStamp(ParseUtils.parseTimestamp(sequencePayload).getTime)
        }

        timeStamp = if (logFileTimestamp != null && Constants.isValidDate(logFileTimestamp, currentLogCode, currentFileName)) logFileTimestamp else null
        sequencePayload = sigSeqRecMod
        logRecord = LogRecord(currentFileName,
          timeStamp, x.get(0).asInstanceOf[Int],
          currentLogCode.toString,
          "",
          logCodeVersion, currentDate, x.getLong(x.fieldIndex("id")).toInt)
      }
      else {
        currentLogCode = ParseUtils.intFromBytes(sequencePayload(3), sequencePayload(2))
        val parsedTimeStampVal = Try(ParseUtils.parseTimestamp(sequencePayload)).getOrElse(null)
        if (parsedTimeStampVal != null) {
          logFileTimestamp = Constants.toUTCTimeStamp(parsedTimeStampVal.getTime)
          timeStamp = if (Constants.isValidDate(parsedTimeStampVal, currentLogCode, currentFileName)) logFileTimestamp else null
          logRecord = LogRecord(x.get(2).asInstanceOf[String],
            timeStamp, x.get(0).asInstanceOf[Int],
            "",
            "",
            ParseUtils.intFromByte(sequencePayload(12)), currentDate, x.getLong(x.fieldIndex("id")).toInt)
          logCodeVersion = ParseUtils.intFromByte(sequencePayload(12))
        }
      }

      if (timeStamp != null) {
        try {
          val exceptionUseArray: List[String] = List(currentFileName, currentLogCode.toString, 0.toString)
          currentLogCode match {
            case _ =>
              if (logcodes_asn_set contains currentLogCode) {
                logRecord = LogRecordParserASN.parseLogRecordASNSet1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_qcomm2_set1 contains currentLogCode) {
                logRecord = LogRecordParserQComm2.parseLogRecordQCOMM2_Set1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray, config)
              } else if (logcodes_qcomm2_set2 contains currentLogCode) {
                logRecord = LogRecordParserQComm2.parseLogRecordQCOMM2_Set2(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm2_set3 contains currentLogCode) {
                logRecord = LogRecordParserQComm2.parseLogRecordQCOMM2_Set3(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_qcomm5g_set1 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm5g_set2 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet2(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm5g_set3 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet3(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm5g_set6 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet6(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm5g_set7 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet7(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm5g_set5 contains currentLogCode) {
                logCodeVersion = ParseUtils.intFrom4Bytes(sequencePayload(15), sequencePayload(14), sequencePayload(13), sequencePayload(12))
                logRecord = LogRecordParser5G.parseLogRecordQCOMM5GSet5(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
                //logger.info(s"LogRecordParser5G >>>>>>>> ${logRecord.logCode} and ${logRecord.version} and testId=${logRecord.testId}")
              }
              else if (logcodes_b193_set contains currentLogCode) {
                logRecord = LogRecordParserB193.parseLogRecordB193(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_b192_set contains currentLogCode) {
                logRecord = LogRecordParserB192.parseLogRecordB192(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_qcomm_set1 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set2 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set2(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set3 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set3(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set4 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set4(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set5 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set5(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set6 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set6(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set7 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set7(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              } else if (logcodes_qcomm_set8 contains currentLogCode) {
                logRecord = LogRecordParserQComm.parseLogRecordQCOMM_Set8(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_nas_set contains currentLogCode) {
                logRecord = LogRecordParserNAS.parseLogRecordNASSet1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_ip_set contains currentLogCode) {
                logRecord = LogRecordParserIP.parseLogRecordIPSet1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if (logcodes_sig_set contains currentLogCode) {
                logRecord = LogRecordParserSIG.parseLogRecordSIGSet1(currentLogCode, logRecord, logCodeVersion, sequencePayload, exceptionUseArray)
              }
              else if(logcodes_trace_set contains currentLogCode) {
                logRecord = LogRecordParseTrace.parseLogRecordTraceSet(currentLogCode,logRecord,logCodeVersion,sequencePayload,exceptionUseArray)
                // logger.info(s"LogRecordTrace >>>>>>>> ${logRecord.logCode} and ${logRecord.version} and lteBlindScanKpiList=${logRecord.logRecordTrace.lteBlindScanKpiList} and lteTopNSignalkpiList=${logRecord.logRecordTrace.lteTopNSignalkpiList}")
              }
          }
        }
        catch {
          case npe: NullPointerException => {
            logger.error("Null Pointer Exception occurred while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, npe.getMessage)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Null Pointer Exception occurred , " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + npe.getMessage)
            logger.error("StackTrace for Null Pointer Exception:{}", npe.getStackTrace)
          }
          case aiob: ArrayIndexOutOfBoundsException => {
            logger.error("Array Index Out of bound Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, aiob.getMessage)
            //logger.error("StackTrace for Array Index Out Of Bound:{}",aiob.getStackTrace)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Array Index Out of bound Exception occurred , " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + aiob.getMessage)
          }
          case ae: ArithmeticException => {
            logger.error("Arithmetic Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, ae.getMessage)
            //logger.error("StackTrace for Arithmetic Exception:{}",ae.getStackTrace)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Arithmetic Exception occurred, " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + ae.getMessage)
          }
          case cce: ClassCastException => {
            logger.error("Class Cast Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, cce.getMessage)
            //logger.error("StackTrace for Arithmetic Exception:{}",cce.getStackTrace)
            logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
              exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Class Cast Exception occurred, " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + cce.getMessage)
          }
          case ge: Exception => {
            if (ge.getMessage != null && ge.getMessage == "Internal Parse Exception occured.") {
              logger.error("Internal Parse Exception occured while processing file:{},{},{}.Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, ge.getMessage)
              logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
                exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive Internal Parse Exception occurred, " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + ge.getMessage)
            }
            else {
              if (config.LOG_GENERAL_EXCEPTION.equalsIgnoreCase("true"))
                logger.error("General Exception occured while processing file:{},{},{}. Exception details : {}", currentFileName, currentLogCode.toString, logCodeVersion.toString, ge.getMessage)
              logRecord = logRecord.copy(logCode = currentLogCode.toString, hexLogCode = "0x" + currentLogCode.toHexString.toUpperCase,
                exceptionOccured = true, fileName = currentFileName, exceptionCause = "SequenceToHive General Exception occurred, " + currentLogCode.toHexString + "," + currentLogCode.toString + "," + logCodeVersion.toString + " Details : " + ge.getMessage)
            }
            logger.error("StackTrace for General Exception:{}", ge.getStackTrace)
          }
        }
      }
      logRecord
    })

    import spark.implicits._

    val dlfListDF = dlfList.toDS().filter($"hexLogCode" =!= "")
    if (dlfListDF != null) {

      val finalData2Hive = dlfListDF.select(flattenSchema(dlfListDF.schema): _*)

      if (paramsConfig.hiveCntChk == "Y") {
        var processParams: ProcessParams = ProcessParams(paramsConfig.id, "hive")
        if (!checkTableExists(spark, config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)) {
          spark.sql(HiveTableStructure.generateHiveSchema("HIVE", config.HIVE_DB, config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek, config.HDFS_URI))
          finalData2Hive.filter($"missingVersion" === -1).write.partitionBy("fileName", "insertedDate", "testId").mode(SaveMode.Overwrite).option("path", config.HIVE_HDFS_PATH + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek).saveAsTable(config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)
          //          processParams = processParams.copy(struct_chng = "N", cnt_chk = "N")
        }
        //        else {
        //          processParams = processParams.copy(struct_chng = "EXIST")
        //        }
        //        CommonDaoImpl.updateProcessParams(processParams, config)
      }
      else {
        if (paramsConfig.hiveStructChng == "Y" && paramsConfig.hiveCntChk == "N") {
          val dataExists = checkTableDataExists(spark, config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)
          finalData2Hive.filter($"missingVersion" === -1).write.partitionBy("fileName", "insertedDate", "testId").mode(if (!dataExists) SaveMode.Overwrite else SaveMode.Append).option("path", config.HIVE_HDFS_PATH + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek).saveAsTable(config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)
          //          if (finalData2Hive.count > 0) {
          //            val processParams: ProcessParams = ProcessParams(paramsConfig.id, "hive", "N", "N")
          //            CommonDaoImpl.updateProcessParams(processParams, config);
          //          }
        }
        else {
          if (paramsConfig.hiveCntChk == "N") {
            finalData2Hive.filter($"missingVersion" === -1)
              .write
              .partitionBy("fileName", "insertedDate", "testId")
              .mode(SaveMode.Append)
              .option("path", config.HIVE_HDFS_PATH + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)
              .saveAsTable(config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek)
          }
          else {
            logger.info("Error at FINAL_HIVE table structure definition. ReCheck HIVE DB")
          }
        }
      }

      import org.apache.spark.sql.functions._

      val pathToHiveTable = config.HIVE_DB + config.FINAL_HIVE_TBL_NAME + paramsConfig.currentWeek
      val idList = logsTblDs.select("id").rdd.map(r => r(0)).collect.toList.mkString(",")

      var logFileInfoData = spark.sql(s"select distinct testId as id, fileName, email, firstName, lastName, mdn, imei_QComm2, udid, modelName, dmUser, isInBuilding from $pathToHiveTable where testId in ($idList) and email <> ''")


      // Create dataframe filled with user details and stubbed device data for files other than dlf ones
      val ccp = config
      val ndUserDetails = logsTblDs.filter(lower($"filetype").isin(Constants.CONST_DML_DLF, Constants.CONST_DRM_DLF, Constants.CONST_SIG_DLF, Constants.CONST_SIG, Constants.CONST_SPI_DLF)).map(x => {
        val fileName = x.getString(x.fieldIndex("fileName"))
        val fileExt = FilenameUtils.getExtension(Constants.getDlfFromSeq(fileName)).toLowerCase
        var logRecord: LogRecord = LogRecord(fileName, Constants.getPGTimeStamp(new Date), -1, "", "", -1, currentDate, x.getLong(x.fieldIndex("id")).toInt)
        fileExt.toLowerCase match {
          case Constants.CONST_DML_DLF =>
            logRecord = LogRecordParserQComm2.getDmlDlfDeviceInfo(logRecord, ccp)
          case Constants.CONST_DRM_DLF =>
            logRecord = LogRecordParserQComm2.getDrmDlfDeviceInfo(logRecord, ccp)
          case Constants.CONST_SIG_DLF | Constants.CONST_SIG =>
            logRecord = LogRecordParserQComm2.getSigDlfDeviceInfo(logRecord, ccp)
          case Constants.CONST_SPI_DLF =>
            logRecord = LogRecordParserQComm2.getSpiDlfDeviceInfo(logRecord, ccp)
          case _ =>
        }
        val mdn = x.getString(x.fieldIndex("mdn"))
        val imei = x.getString(x.fieldIndex("imei"))
        Row(logRecord.testId,
          logRecord.fileName,
          logRecord.logRecordQComm2.email,
          logRecord.logRecordQComm2.firstName,
          logRecord.logRecordQComm2.lastName,
          if (StringUtils.isNotBlank(mdn)) mdn else logRecord.logRecordQComm2.mdn,
          if (StringUtils.isNotBlank(imei)) imei else logRecord.logRecordQComm2.imei_QComm2,
          logRecord.logRecordQComm2.udid,
          logRecord.logRecordQComm2.modelName,
          logRecord.logRecordQComm2.dmUser,
          logRecord.logRecordQComm2.isInBuilding)
      }
      )(RowEncoder.apply(logFileInfoData.schema))

      val ignoreNulls = true
      val nonNullExprs = logFileInfoData.columns.filter(!Seq("id", "fileName").contains(_)).map(first(_, ignoreNulls))

      // Take only non-null values for any column, rename aggregation result columns to original ones
      logFileInfoData = logFileInfoData.unionByName(ndUserDetails).groupBy("id", "fileName").agg(nonNullExprs.head, nonNullExprs.tail: _*).toDF(logFileInfoData.columns: _*)

      val getDmUser = udf((s: Int) => if (s == null || s == 0) 10088 else s)
      val strBoolToInt = udf((s: String) => if (s != null && s.toUpperCase == "TRUE") 1 else 0)
      resDf = logsTblDs.join(
          logFileInfoData.select(
            $"id" as "id",
            $"fileName" as "fileName",
            $"mdn" as "mdn_new",
            $"imei_QComm2" as "imei_new",
            $"firstName" as "firstName_new",
            $"lastName" as "lastName_new",
            $"email" as "email_new",
            $"modelName" as "modelName_new",
            $"udid" as "udid_new",
            getDmUser($"dmUser") as "dmUser_new",
            strBoolToInt($"isInBuilding") as "isinbuilding_new"
          ), Seq("id", "fileName"), "left_outer")
        .withColumn("mdn", $"mdn_new")
        .withColumn("imei", $"imei_new")
        .withColumn("firstname", lit($"firstName_new"))
        .withColumn("lastname", lit($"lastName_new"))
        .withColumn("emailid", lit($"email_new"))
        .withColumn("modelName", lit($"modelName_new"))
        .withColumn("udid", lit($"udid_new"))
        .withColumn("dmUser", lit($"dmUser_new"))
        .withColumn("isinbuilding", lit($"isinbuilding_new"))
        .drop("fileName_new", "mdn_new", "imei_new", "firstName_new", "lastName_new", "email_new", "modelName_new", "udid_new", "dmUser_new", "isinbuilding_new")
        .setNullableStateExcept("id")

      resDf = resDf
        .withColumn("status", lit(Constants.HIVE_SUCCESS))
        .withColumn("hiveEndTime", lit(System.currentTimeMillis()))
        .withColumn("reportsHiveTblName", functions.lit(paramsConfig.currentWeek))
      if (config.ODLV_TRIGGER) resDf = resDf.withColumn("odlvOnpStatus", lit(Constants.HIVE_SUCCESS))

      if (config.SEQ_FILE_DEL.toLowerCase == "true") {
        CommonDaoImpl.deleteSeqFiles(resDf.filter(($"fileName" like ("%" + Constants.CONST_DRM_DLF + "%")) || ($"fileName" like ("%" + Constants.CONST_DML_DLF + "%"))), config)
      }
      kafkaSink.writeBatch(
        resDf.setNullableStateExcept("id"),
        config.ES_KAFKA_TOPIC, config.NOTIFICATION_KAFKA_TOPIC
      )
    }
    resDf
  }

  def checkTableExists(spark: SparkSession, tableName: String) = {
    //logger.info("Checking for Table Exists : " + spark.catalog.tableExists(tableName).toString)
    spark.catalog.tableExists(tableName)
  }

  def checkTableDataExists(spark: SparkSession, tableName: String) = {
    val countDS = spark.sql("select count(*) as tableCount from " + tableName).select("tableCount").collectAsList()
    val tableCountAsString = countDS.get(0).get(0).toString
    //println("tableCountAsString>>>>>>>>>>>"+tableCountAsString)
    tableCountAsString != null && tableCountAsString != "0"
    //false
  }

  private def extractFileNames(fullPaths: String) = {
    if (fullPaths == null || fullPaths.isEmpty) {
      Array[String]()
    } else {
      val paths = fullPaths.split(',')
      paths.map(p => p.substring(p.lastIndexOf('/') + 1))
    }
  }

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }
}
