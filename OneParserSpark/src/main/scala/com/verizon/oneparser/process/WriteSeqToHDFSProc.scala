package com.verizon.oneparser.process

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.zip.ZipInputStream
import java.util.{Calendar, Date}
import com.dmat.qc.parser.{IpConstruct, IpUtils}
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.SequenceObj
import com.verizon.oneparser.common.{Constants, LogStatus, OPutil, SipInfoDto}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.dto.Logs
import com.verizon.oneparser.odlv.LogViewerHandler
import com.verizon.oneparser.utils.{DateUtils, RunStatistics, ZipExtractor}
import com.vzwdt.RootMetrics.ADBMessageParse
import com.vzwdt.ip.{IpDirection, SipHeader}
import com.vzwdt.parseutils.ParseUtils
import net.liftweb.json.parse
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IOUtils, IntWritable, SequenceFile}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json.JSONObject

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * WriteSeqToHDFSProc log file processor
 */
case object WriteSeqToHDFSProc extends LazyLogging {
  val sourceFullPathFieldName = "sourceFullPath"
  val seqFullPathFieldName = "seqFullPath"

  def unit(spark: SparkSession, config: CommonConfigParameters, sourceDF: DataFrame = null): WriteSeqToHDFSProc = WriteSeqToHDFSProc(spark, config, sourceDF)
}

case class WriteSeqToHDFSProc(spark: SparkSession, config: CommonConfigParameters, df: DataFrame) extends LazyLogging {

  import spark.implicits._

  /**
   * Returns current DataFrame
   *
   * @return current DataFrame
   */
  def getDataFrame = df

  /**
   * Updates log status to SEQ_IN_PROGRESS
   *
   * @return current DataFrame
   */
  def preprocess: DataFrame = {
    preprocess(df)
  }

  /**
   * Updates log status to SEQ_IN_PROGRESS in specified DataFrame
   *
   * @param df specified DataFrame
   * @return current DataFrame
   */
  def preprocess(df: DataFrame): DataFrame = {
    def preprocess(logs: Logs) =  {
      var logsUpdated = logs.copy(
        status = LogStatus.SEQ_IN_PROGRESS,
        seqStartTime = System.currentTimeMillis()
      )
      if (config.ODLV_TRIGGER) logsUpdated = logsUpdated.copy(odlvOnpStatus = LogStatus.SEQ_IN_PROGRESS)
      logsUpdated
    }
    df.as[Logs].mapPartitions(partition => iterate(partition, preprocess, "preprocess", true)).toDF
  }

  private def iterate(partition: Iterator[Logs], func: Logs => Logs, msg: String = null, silent: Boolean = false) = {
    if (silent) {
      partition.map(log => {
        func(log)
      })
    } else {
      val startTime = System.currentTimeMillis()
      val logFileNames = new ListBuffer[String]()
      logger.warn(s"${msg} - Partition ${TaskContext.getPartitionId} Start time: ${DateUtils.millisToTimestamp(startTime)}.")
      val result = partition.map(log => {
        logger.warn(s"${msg} - Partition: ${TaskContext.getPartitionId}, fileName: ${log.fileName}")
        val logResult: Logs = func(log)
        logFileNames += log.fileName
        if (!partition.hasNext) {
          val endTime = System.currentTimeMillis()
          logger.warn(s"${msg} - Partition ${TaskContext.getPartitionId} End time: ${DateUtils.millisToTimestamp(endTime)}.")
          val elapsedTime = endTime - startTime;
          val averageTime = if (logFileNames != null && logFileNames.size != 0) elapsedTime / logFileNames.size else 0
          logger.warn(s"${msg} - Partition ${TaskContext.getPartitionId} Elapsed time: ${elapsedTime} ms.; Batch size: ${logFileNames.size}; Average elapsed time ${averageTime}; Processed: ${logFileNames.toList.mkString(", ")}.")
        }
        logResult
      })
      result
    }
  }

  /**
   * Starts processing based on specified DataFrame
   *
   * @return current DataFrame
   */
  def process: DataFrame = {
    def repartitionOpt(df: DataFrame): DataFrame = if(config.WRITE_SEQ_PARTITIONS > 1) df.repartition(config.WRITE_SEQ_PARTITIONS, $"fileName") else df
    repartitionOpt(df).as[Logs]
      .mapPartitions(partition => iterate(partition, process, "process"))
      .filter($"status" =!= LogStatus.DLF_DOES_NOT_EXIST).toDF()
  }

  private def process(log: Logs) = {
    val sourcePath = sourceFileFullPath(log.fileName, log.fileLocation, log.updatedTime)
    logger.info(s"Current Sequencing File : $sourcePath")
    val logProc = writeSequenceFile(log, sourcePath)
    logger.info("Finished WriteSequenceToHDFS process ==>>")
    logProc
  }

  private def getDirDateFromPath(filePath: String) = {
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    val path_split = filePath.split("\\/processed")
    var date = dateFormatter.format(new Date())
    if (path_split.nonEmpty) {
      val fileLoc_splt = path_split.apply(0).split("/")
      if (fileLoc_splt.nonEmpty) {
        val len = fileLoc_splt.length;
        date = fileLoc_splt.apply(len - 1)
      }
    }
    date
  }

  private def writeSequenceFile(log: Logs, sourcePath:String): Logs = {
    val broadCastValue = getHDFSBroadcastConfig
    val outputPath = Constants.getHDFSDirPath("SEQ", "", getDirDateFromPath(sourcePath), config.HDFS_FILE_DIR_PATH)
    val fs: FileSystem = FileSystem.get(broadCastValue)
    val filePath = new Path(sourcePath)
    var logProc: Logs = null
    if (fs.exists(filePath)) {
      try {
        val dlfData = fs.open(filePath)
        if (sourcePath.contains(".zip")) {
          logger.info("The file need to Be Decompress is --- " + sourcePath)
          val zipExtractor: ZipExtractor = new ZipExtractor
          logger.info("Decompressing and writing DLF normally")
          val zipOutputPath = Constants.getHDFSDirPath("DLF", "", getDirDateFromPath(sourcePath), config.HDFS_FILE_DIR_PATH)
          val zipExtractorResult = zipExtractor.extractZipFileIntoHdfs(new ZipInputStream(dlfData), broadCastValue, zipOutputPath)
          val entryName = zipExtractorResult.entryName
          logger.info("The Value of Zip Entry" + entryName)
          logger.info("Decompressing and writing DLF normally End")
          val fso: FSDataInputStream = fs.open(new Path(zipOutputPath + entryName))
          // entryName is only file name within archive without relative path
          val seqFileName = Constants.getSeqFromDlf(entryName)
          logProc = RunStatistics.time(
            run(fso, log, seqFileName, broadCastValue, outputPath),
            s"Processed: $seqFileName"
          )
          //update zip extraction time
          logProc = logProc.copy(zipStartTime = zipExtractorResult.startTime, zipEndTime = zipExtractorResult.endTime)

          deleteFile(zipOutputPath + entryName, broadCastValue)
        } else {
          // path contains path delimiters
          val seqFileName = getSeqFileName(sourcePath)
          logProc = RunStatistics.time(
            run(dlfData, log, seqFileName, broadCastValue, outputPath),
            s"Processed: $seqFileName"
          )
        }
        if (config.ODLV_TRIGGER) logProc = logProc.copy(odlvOnpStatus = logProc.status, odlvLvStatus = "READY", logfilestatusforlv = "READY")
        if (config.LOG_FILE_DEL) {
          deleteFile(sourcePath, broadCastValue)
        }
      } catch {
        case e: FileNotFoundException =>
          logger.error("Exception Processing File: " + log.fileName, e)
          logProc = log.copy(status = LogStatus.SEQ_FAILED, esDataStatus = LogStatus.FAILED)
      }
    } else {
      logger.warn(s"File $sourcePath does not exist")
      logProc = log.copy(status = LogStatus.DLF_DOES_NOT_EXIST)
    }
    logProc
  }

  private def getSeqFileName(filePath: String) = {
    val compositeFormat = s"""(.*)/(.*).(${Constants.CONST_DRM_DLF}|${Constants.CONST_DML_DLF}|${Constants.CONST_SIG_DLF})""".r
    val sigFormat = s"""(.*)/(.*).(${Constants.CONST_SIG})""".r
    val dlfFormat = s"""(.*)/(.*).(${Constants.CONST_DLF})""".r
    filePath match {
      case compositeFormat(_, fileNameWithoutExtension, extension, _*) => fileNameWithoutExtension + "_" + extension + ".seq"
      case sigFormat(_, fileNameWithoutExtension, _*) => fileNameWithoutExtension + "_sig.seq"
      case dlfFormat(_, fileNameWithoutExtension, _*) => fileNameWithoutExtension + ".seq"
      case _ => throw new Exception("Unsupported log format")
    }
  }

  private def run(din: FSDataInputStream, log: Logs, seqFileName: String, broadCastValue: org.apache.hadoop.conf.Configuration, outputPath: String): Logs = {
    var startLogTime: Date = null
    var endLogTime: Date = null
    logger.warn("START Processing Thread for file: " + outputPath + seqFileName)
    var endOfFile = false
    var seqObj: SequenceObj = SequenceObj(seqFileName = seqFileName)
    var noOfRecordsVal = 0
    var status = LogStatus.SEQ_DONE
    var failureInfo: String = null
    var esDataStatus: String = log.esDataStatus
    var mdn: String = log.mdn
    var imei: String = log.imei
    try {
      var writer: SequenceFile.Writer = null
      try {
        writer = SequenceFile.createWriter(
          broadCastValue,
          SequenceFile.Writer.file(new Path(outputPath + seqFileName)),
          SequenceFile.Writer.keyClass(classOf[IntWritable]),
          SequenceFile.Writer.valueClass(classOf[BytesWritable])
        )
        logger.info("Current File Name : " + seqFileName)
        val isSigFile = seqFileName.contains("_sig.seq")
        while (!endOfFile) {
          seqObj = if (isSigFile) nextRecordSig(din, writer, seqObj, config.IP_PARSER_JAR_SUPPORT) else nextRecord(din, writer, seqObj, config.IP_PARSER_JAR_SUPPORT)
          endOfFile = seqObj.endOfFile
          noOfRecordsVal = noOfRecordsVal+1
          if (seqObj.LcTime != null) {
            if (startLogTime == null) {
              startLogTime = seqObj.LcTime
            }
            endLogTime = seqObj.LcTime
          }
        }
      }
      catch {
        case ex: IOException =>
          logger.error("Sequence Data: Error! " + ex)
          status = LogStatus.FAILED
      }
      finally {
        logger.warn("Stream is closed")
        IOUtils.closeStream(writer)
        din.close()
      }

      if (noOfRecordsVal <= 8) {
        esDataStatus = LogStatus.FAILED
        status = LogStatus.SEQ_FAILED
        failureInfo = "Cannot Process the file with " + seqObj.logCodesSet.size + " Records"
      } else {
        if (seqFileName.contains(Constants.CONST_DML_DLF)) {
          if (seqObj.rmMdn.nonEmpty) mdn = seqObj.rmMdn
          if (seqObj.rmDeviceId.nonEmpty) imei = seqObj.rmDeviceId
        }
        logger.info("Total No Of Records Found for file : " + seqFileName + " is : " + noOfRecordsVal)
        if (config.ODLV_TRIGGER && StringUtils.isNoneEmpty(config.LOG_VIEWER_URL)) {
          LogViewerHandler.initiateODLV(config.LOG_VIEWER_URL, log)
        }
      }
    } catch {
      case e: Exception =>
        if (din != null) try {
          logger.error("Input Data stream was not null but job caught in try catch of run block")
          failureInfo = "Input Data stream was not null but job caught in try catch of run block. Details: " + e.getCause
          din.close()
        } catch {
          case e2: Exception =>
            logger.error("ERROR Closing InputStream to file :" + " - " + e2.getMessage)
            failureInfo = "ERROR Closing InputStream to file. Details: " + e2.getCause
        }
        logger.error("Exception Processing File: " + e.getMessage + " with testid " + log.id)
        esDataStatus = LogStatus.FAILED
        status = LogStatus.SEQ_FAILED
    }
    logger.info("FINISHED Processing Thread for file : " + seqFileName + " ==>> ")

    var lvupdatedtime = log.lvupdatedtime
    var lvlogfilecnt = log.lvlogfilecnt
    var hasgps = log.hasgps
    var technology = log.technology

    if (seqObj.logCodesSet.size > 0) {
      lvupdatedtime = System.currentTimeMillis()
      lvlogfilecnt = noOfRecordsVal
      hasgps = if (seqObj.logCodesSet.exists(Constants.LOGCODES_GPS.contains)) 1 else 0
      /*
       * Meanwhile only 4g and 5g technologies available. There might be multiple technologies
       * so it's a collection.
       */
      val technologyArray = new ArrayBuffer[String](2)
      if (seqObj.logCodesSet.exists(Constants.LOGCODES_4G.contains)) technologyArray += "4g"
      if (seqObj.logCodesSet.exists(Constants.LOGCODES_5G.contains)) technologyArray += "5g"
      technology = technologyArray.mkString(",")
    }

    log.copy(
      status = status,
      startLogTime = if (startLogTime == null) null else startLogTime.getTime,
      lastLogTime = if (endLogTime == null) null else endLogTime.getTime,
      seqEndTime = System.currentTimeMillis(),
      fileName = seqFileName,
      failureInfo = failureInfo,
      esDataStatus = esDataStatus,
      lvupdatedtime = lvupdatedtime,
      lvlogfilecnt = lvlogfilecnt,
      hasgps = hasgps,
      technology = technology,
      mdn = mdn,
      imei = imei,
      carrierName = OPutil.getCarrierByFileName(seqFileName),
      logcodes = seqObj.logCodesSet.mkString(","),
      triggerCount = 0,
      userPriorityTime = if (seqObj.is0xFFF3Exists) Instant.now().minus(config.PRIORITY_TIME_INTERVAL.longValue(), ChronoUnit.HOURS).toEpochMilli else log.userPriorityTime
    )
  }

  private def deleteFile(fileName: String, broadCastValue: org.apache.hadoop.conf.Configuration): Unit = {
    val fs: FileSystem = FileSystem.get(broadCastValue)
    val deletePath = fileName
    fs.delete(new Path(deletePath), true)
  }

  private def nextRecord(din: FSDataInputStream, writer: SequenceFile.Writer, seqObj: SequenceObj, useIPJar:Boolean=false) = {
    var sequenceObj: SequenceObj = seqObj
    val key: IntWritable = new IntWritable
    var logRecord: Array[Byte] = null
    sequenceObj = getNextBytes(2, din, sequenceObj)
    val numBytes = sequenceObj.temp

    var timeStamp: Date = null
    if (numBytes != null) {
      val length = ParseUtils.intFromBytes(numBytes(1), numBytes(0))

      if (length > 4) {
        sequenceObj = getNextBytes(2, din, sequenceObj)
        val numBytes1 = sequenceObj.temp
        if (numBytes1 != null) {
          val logCode = ParseUtils.intFromBytes(numBytes1(1), numBytes1(0))
          sequenceObj.logCodesSet += s"0x" + logCode.toHexString.toUpperCase
          sequenceObj = getNextBytes(length - 4, din, sequenceObj)
          val remainder = sequenceObj.temp
          if (remainder != null) {
            val byteBuffer = ByteBuffer.allocate(length)
            byteBuffer.put(numBytes)
            byteBuffer.put(numBytes1)
            byteBuffer.put(remainder)
            logRecord = byteBuffer.array()
            try {
              timeStamp = ParseUtils.parseTimestamp(logRecord)
              if ((logCode < 0xFFF0 || logCode == 0xFFF4 || logCode == 0xFFFB) && isValidDate(timeStamp, logCode)) {
                sequenceObj = sequenceObj.copy(LcTime = timeStamp)
              }
            } catch {
              case ge: Exception =>
                logger.error("Exception Occurred while parsing TimeStamp : " + ge.getMessage)
                logRecord = null
            }
            if (logCode == 0xF0E4 && logRecord != null && sequenceObj.rmDeviceId.isEmpty) {
              val adbMsg = getAdbMsgBeanAsJSON(logRecord)
              sequenceObj = sequenceObj.copy(rmDeviceId = getJsonValueByKey(adbMsg, "device_id"))
            }
            if(logCode == 0xFFF3 && !sequenceObj.is0xFFF3Exists){
              sequenceObj = sequenceObj.copy(is0xFFF3Exists = true)
              logger.info("Found a 0xFFF3 inside the file....")
            }
            if (logCode == 0x11EB && logRecord != null) {
              try {
                sequenceObj.logCodesSet += "0xFFF0"
                if (useIPJar) {
                  val mConstructIpPacket: IpConstruct = new IpConstruct(IpUtils.getInstance)
                  val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)
                  if (ipPacket != null) {
                    logRecord = handleContinuationSipMessage(ipPacket)
                    if (sequenceObj.rmMdn.isEmpty && sequenceObj.seqFileName.contains(Constants.CARRIER_VERIZON) && sequenceObj.seqFileName.contains("_dml_dlf")) {
                      val sipInfoDto: SipInfoDto = getSipExtractedInfo(logRecord)
                      if (sipInfoDto.subtitle.trim.equalsIgnoreCase("invite") && sipInfoDto.direction.equalsIgnoreCase("outgoing")) {
                        if (!sipInfoDto.from.isEmpty) {
                          val fromSpltArr: Array[String] = sipInfoDto.from.split("@").map(_.trim)
                          if (fromSpltArr.size > 0) {
                            sequenceObj = sequenceObj.copy(rmMdn = fromSpltArr(0).replace("<sip:+1", ""))
                          }
                        }
                      }
                    }
                  } else if (ipPacket == null) {
                    val pcapConstructIpPacket: com.verizon.pcap.IpConstruct = new com.verizon.pcap.IpConstruct(com.verizon.pcap.IpUtils.getInstance)
                    val pcapIpPacket = pcapConstructIpPacket.buildIpPacket(logRecord)
                    if (pcapIpPacket != null){
                      sequenceObj.logCodesSet += "0x11EB"
                      logRecord = IpUtils.concatenate(java.util.Arrays.copyOfRange(logRecord,0,14),pcapIpPacket)
                    }
                  }
                }
                else{
                  val mConstructIpPacket: IpConstruct = new IpConstruct(IpUtils.getInstance)
                  val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)
                  if(ipPacket != null) {
                    logRecord = handleContinuationSipMessage(ipPacket)
                    if (sequenceObj.rmMdn.isEmpty && sequenceObj.seqFileName.contains(Constants.CARRIER_VERIZON) && sequenceObj.seqFileName.contains("_dml_dlf")) {
                      val sipInfoDto: SipInfoDto = getSipExtractedInfo(logRecord)
                      if (sipInfoDto.subtitle.trim.equalsIgnoreCase("invite") && sipInfoDto.direction.equalsIgnoreCase("outgoing")) {
                        if (!sipInfoDto.from.isEmpty) {
                          val fromSpltArr: Array[String] = sipInfoDto.from.split("@").map(_.trim)
                          if (fromSpltArr.size > 0) {
                            sequenceObj = sequenceObj.copy(rmMdn = fromSpltArr(0).replace("<sip:+1", ""))
                          }
                        }
                      }
                    }
                  }
                }
              }
              catch {
                case ge:Exception =>
                  //logger.error("Exception Occurred while parsing IP Packet : "+ge.getMessage + " Stack Trace : "+ge.getStackTrace.mkString)
                  logger.error("Exception Occurred while parsing IP Packet : "+ge.getMessage)
                  logRecord = null
              }
            }
            try {
              if (logRecord != null) {
                val value = new BytesWritable
                value.set(logRecord, 0, logRecord.length)
                val idx = seqObj.index
                key.set(idx)
                writer.append(key, value)
                sequenceObj = sequenceObj.copy(index = idx + 1)
              }
            } catch {
              case ex: Exception =>
                logger.error("Sequence Data: Error! " + ex)
            }
          }
        }
      }
      else sequenceObj = sequenceObj.copy(endOfFile = true)
    }
    sequenceObj
  }

  private def nextRecordSig(din: FSDataInputStream, writer: SequenceFile.Writer, seqObj: SequenceObj, useIPJar:Boolean=false): SequenceObj = {
    var sequenceObj: SequenceObj = seqObj

    var key: IntWritable = new IntWritable
    var logRecord: Array[Byte] = null
    sequenceObj = getNextBytes(2, din, sequenceObj)
    // Gets first 2 bytes of inputstream which is record length
    val numBytes = sequenceObj.temp
    val device_types_set = Array(2,8,9,10,15,16,17,18,20,21,23,25,30,31,33,34,36,37,40,41)
    val logcodes_sig_set = Array(8449,9227,9222,9224,9229,9473,8535,9228)

    if (numBytes != null) {
      val length = ParseUtils.intFromBytes(numBytes(1), numBytes(0))

      // The samllest legal value of length is 6(null header and 0-length data)
      if (length > 6) {
        // length of previous record
        sequenceObj = getNextBytes(2, din, sequenceObj)
        val numBytes1 = sequenceObj.temp
        // deviceType of the record in the navigation header
        sequenceObj = getNextBytes(1, din, sequenceObj)
        val numBytes2 = sequenceObj.temp
        val deviceType = ParseUtils.intFromByte(numBytes2(0))
        var remainder: Array[Byte] = null
        var numBytes3: Array[Byte] = null
        var numBytes4: Array[Byte] = null
        var numBytes5: Array[Byte] = null
        var logCode = 0

        if (deviceType == 26 || deviceType == 42) {
          // Unit number of the record in the navigation header
          sequenceObj = getNextBytes(1, din, sequenceObj)
          numBytes3 = sequenceObj.temp
          // Record Length in Qcomm record
          sequenceObj = getNextBytes(2, din, sequenceObj)
          numBytes4 = sequenceObj.temp
          // LogCode in Qcomm record
          sequenceObj = getNextBytes(2, din, sequenceObj)
          numBytes5 = sequenceObj.temp
          logCode = ParseUtils.intFromBytes(numBytes5(1), numBytes5(0))
          sequenceObj.logCodesSet += s"0x" + logCode.toHexString.toUpperCase
          // The remaining records apart from the previous read 5 bytes
          sequenceObj = getNextBytes(length - 10, din, sequenceObj)
          remainder = sequenceObj.temp
        } else {
          // The remaining records apart from the previous read 5 bytes
          sequenceObj = getNextBytes(length - 5, din, sequenceObj)
          remainder = sequenceObj.temp
        }

        if (remainder != null) {
          val byteBuffer = ByteBuffer.allocate(length)
          byteBuffer.put(numBytes)
          byteBuffer.put(numBytes1)
          byteBuffer.put(numBytes2)

          if (deviceType == 26 || deviceType == 42) {
            byteBuffer.put(numBytes3)
            byteBuffer.put(numBytes4)
            byteBuffer.put(numBytes5)
          }

          byteBuffer.put(remainder)
          logRecord = byteBuffer.array()

          if(device_types_set contains deviceType) {
            try{
              val sigSeqRecord = logRecord
              val sigSeqRecMod = OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(sigSeqRecord)
              val currentLogCode = ParseUtils.intFromBytes(sigSeqRecMod(2), sigSeqRecMod(3))
              if(logcodes_sig_set contains currentLogCode) {
                sequenceObj.logCodesSet += s"0x" + currentLogCode.toHexString.toUpperCase
                val dmTimeStamp: java.sql.Timestamp = new java.sql.Timestamp(OPutil.calculateHbflexDmTimestamp(sigSeqRecMod))
                if (isValidDate(dmTimeStamp, currentLogCode)) {
                  sequenceObj = sequenceObj.copy(LcTime = dmTimeStamp)
                }
              }
            }
            catch {
              case ge:Exception =>
                logger.error("Exception Occurred while parsing SIG File's TimeStamp : "+ge.getMessage)
                logRecord = null
            }

          }

          if (deviceType == 26 || deviceType == 42) {
            if (logCode == 0x11EB && logRecord!=null) {
              try{
                sequenceObj.logCodesSet += "0xFFF0"
                if (useIPJar) {
                  val mConstructIpPacket: IpConstruct = new IpConstruct(IpUtils.getInstance)
                  val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)
                  if (ipPacket != null) {
                    logRecord = handleContinuationSipMessage(ipPacket)
                  } else if (ipPacket == null) {
                    val pcapConstructIpPacket: com.verizon.pcap.IpConstruct = new com.verizon.pcap.IpConstruct(com.verizon.pcap.IpUtils.getInstance)
                    val pcapIpPacket = pcapConstructIpPacket.buildIpPacket(logRecord)
                    if (pcapIpPacket != null) {
                      sequenceObj.logCodesSet += "0x11EB"
                      logRecord = IpUtils.concatenate(java.util.Arrays.copyOfRange(logRecord,0,14),pcapIpPacket)
                    }
                  }
                }
                else{
                  val mConstructIpPacket: IpConstruct = new IpConstruct(IpUtils.getInstance)
                  val ipPacket = mConstructIpPacket.buildIpPacket(logRecord)
                  if(ipPacket != null) {
                    logRecord = handleContinuationSipMessage(ipPacket)
                  }
                }
              }
              catch {
                case ge:Exception =>
                  //logger.error("Exception Occurred while parsing IP Packet : "+ge.getMessage + " Stack Trace : "+ge.getStackTrace.mkString)
                  logger.error("Exception Occurred while parsing SIG file --> IP Packet : "+ge.getMessage)
                  logRecord = null
              }
            }
          }

          try {
            if (logRecord != null) {
              val value = new BytesWritable
              value.set(logRecord, 0, logRecord.length)

              val idx = seqObj.index
              key.set(idx)
              writer.append(key, value)
              sequenceObj = sequenceObj.copy(index = idx + 1)
            }
          } catch {
            case ex =>
              logger.error("Sequence Data in nextRecord method : Error! " + ex.getMessage)
          }
        }
      } else sequenceObj = sequenceObj.copy(endOfFile = true)
    }

    sequenceObj
  }

  private def getNextBytes(num: Int, din: FSDataInputStream, seqObj: SequenceObj) = {
    var sequenceObj: SequenceObj = seqObj
    val tempLocal = new Array[Byte](num)
    try {
      din.readFully(tempLocal)
      val bufPtrLocal = seqObj.bufPtr
      sequenceObj = sequenceObj.copy(temp = tempLocal, bufPtr = bufPtrLocal + num)
    } catch {
      case e: Exception => {
        sequenceObj = sequenceObj.copy(temp = null, endOfFile = true)
        logger.info("Exception in Thread in getNextBytes :  - " + e.getMessage)
      }

    }
    sequenceObj
  }

  private def isValidDate(timeStamp: Date, logCode: Integer) = {
    var isValidDate = false
    val calendarInstance1 = Calendar.getInstance
    calendarInstance1.setTime(new Date)
    calendarInstance1.add(Calendar.DATE, 1)
    val currentDate = calendarInstance1.getTime
    val calendarInstance2 = Calendar.getInstance
    calendarInstance2.setTime(timeStamp)
    val logcodeYear = calendarInstance2.get(Calendar.YEAR)
    if ((logcodeYear >= 2016) && (timeStamp.compareTo(currentDate) <= 0)) {
      isValidDate = true
    }
    else {
      logger.info("Warning: Invalid Date Present!: " + logcodeYear + "; LogCode: " + logCode)
    }
    isValidDate
  }

  private def getHDFSBroadcastConfig: Configuration = {
    val broadcastConfig: Configuration = new Configuration()
    broadcastConfig.addResource(new Path(config.PATH_TO_CORE_SITE))
    broadcastConfig.addResource(new Path(config.PATH_TO_HDFS_SITE))
    broadcastConfig
  }

  private def sourceFileFullPath(fileName: String, fileLocation: String, updatedTime: Long): String = {
    Constants.getDlfFilePathFromLoc(fileName, fileLocation, DateUtils.millisToString(updatedTime), config.HDFS_FILE_DIR_PATH)
  }

  private def seqFullPath(fileName: String, fileLocation: String, updatedTime: Long): String = {
    Constants.getSeqFilePathFromDlf(fileName, fileLocation, DateUtils.millisToString(updatedTime), config.HDFS_FILE_DIR_PATH)
  }

  def getJsonValueByKey(jsonStr: String, jsonColName: String) = {
    (parse(jsonStr) \\ jsonColName).values.getOrElse(jsonColName, "").toString
  }

  def getSipBeanAsJSON(row: Array[Byte], pkt_13: Int) = {
    var sipResponse: String = ""
    var parseRec: SipHeader = null
    if (pkt_13 == 0) {
      parseRec = new SipHeader(row, IpDirection.OUTGOING)
    } else if (pkt_13 == 1) {
      parseRec = new SipHeader(row, IpDirection.INCOMING)
    } else {
      parseRec = new SipHeader(row, IpDirection.UNKNOWN)
    }
    if (parseRec != null) {
      parseRec.parseHeader
      var jsonObject: JSONObject = parseRec.getBean
      if (jsonObject != null) sipResponse = jsonObject.toString
    }
    else {
      logger.error("Unable to parse SIP Bean. Packet[13] info : " + pkt_13.toString)
    }
    //logger.info("SIP JSON ==>"+sipResponse)
    sipResponse
  }

  def handleContinuationSipMessage(inputBytes: Array[Byte]): Array[Byte] = {
    var logRecord = inputBytes
    val pkt_13: Int = ParseUtils.intFromByte(logRecord(13))
    val pkt_12: Int = ParseUtils.intFromByte(logRecord(12))
    val subArrayBytes_SR = java.util.Arrays.copyOfRange(logRecord, 14, logRecord.length)
    if (pkt_12 == 0) {
      val sipJsonBean: String = getSipBeanAsJSON(subArrayBytes_SR, pkt_13)
      val sipSubtitle = getJsonValueByKey(sipJsonBean, "Subtitle")
      if (sipSubtitle.toLowerCase.trim.equalsIgnoreCase("continuation")) {
        logRecord = null
      }
    }
    logRecord
  }

  def getSipExtractedInfo(inputBytes: Array[Byte]): SipInfoDto = {
    var logRecord = inputBytes
    val pkt_13: Int = ParseUtils.intFromByte(logRecord(13))
    val pkt_12: Int = ParseUtils.intFromByte(logRecord(12))
    val subArrayBytes_SR = java.util.Arrays.copyOfRange(logRecord, 14, logRecord.length)
    var sipSubtitle = ""
    var sipFrom = ""
    var sipDirection = ""
    if (pkt_12 == 0) {
      val sipJsonBean: String = getSipBeanAsJSON(subArrayBytes_SR, pkt_13)
      sipSubtitle = getJsonValueByKey(sipJsonBean, "Subtitle")
      sipFrom = getJsonValueByKey(sipJsonBean, "From")
      sipDirection = getJsonValueByKey(sipJsonBean, "Direction ")
      if (sipDirection.isEmpty) {
        sipDirection = getJsonValueByKey(sipJsonBean, "Direction")
      }
      if (sipSubtitle.toLowerCase.trim.equalsIgnoreCase("continuation")) {
        logRecord = null
      }
    }
    val sid: SipInfoDto = new SipInfoDto(sipFrom, sipSubtitle, sipDirection)
    sid
  }

  def getAdbMsgBeanAsJSON(row: Array[Byte]) = {
    var adbMsgResponse: String = ""
    val x = row
    var parseRec: ADBMessageParse = new ADBMessageParse
    try {
      parseRec.parse(x, true)
      var jsonObject: JSONObject = parseRec.getBean
      if (jsonObject != null) adbMsgResponse = jsonObject.toString
    } catch {
      case ae: ArrayIndexOutOfBoundsException =>
        logger.info("Sequence Job : ArrayIndexOutOfBoundsException Occurred while Parsing RM Test Info : " + ae.getCause)
      case iae: IllegalArgumentException =>
        logger.info("Sequence Job : IllegalArgumentException Occurred while Parsing RM Test Info : " + iae.getCause)
      case ge: Exception =>
        logger.info("Sequence Job : General Exception Occurred while Parsing RM Test Info : " + ge.getCause)
    }
    adbMsgResponse
  }

}
