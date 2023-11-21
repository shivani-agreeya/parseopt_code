package com.verizon.oneparser.common

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, InputStream}
import java.text.{ParseException, SimpleDateFormat}
import java.time.LocalDate
import java.util.zip.{ZipEntry, ZipInputStream}
import java.util.{Calendar, Date, GregorianCalendar}

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.config.CommonConfigParameters
import com.vzwdt.parseutils.ParseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try
object OPutil extends LazyLogging {

  val AVAILABLE_CARRIERS = Array("AT&T","Sprint","T-Mobile","Verizon","GCI")
  val CARRIER_SCANNER = "Scanner"
  val CARRIER_TRACE = "Trace"
  val DRM_AVAILABLE_CARRIERS = Map("0D"->"T-Mobile","0E"->"Verizon","0F"->"Sprint","10"->"AT&T")

  def getCellSiteIndices(cellSiteIndex: String, cellSiteWeeklyIndexDate: String, startDate: String, endDate: String): String = {
    var cellSiteIndices = ""
    val startDateSplit = startDate.split("-")
    val endDateSplit = endDate.split("-")
    val cutOffDateSplit = cellSiteWeeklyIndexDate.split("-")
    val localStartDate = LocalDate.of(startDateSplit(0).toInt, startDateSplit(1).toInt, startDateSplit(2).toInt)
    val localEndDate = LocalDate.of(endDateSplit(0).toInt, endDateSplit(1).toInt, endDateSplit(2).toInt)
    val localCutOffDate = LocalDate.of(cutOffDateSplit(0).toInt, cutOffDateSplit(1).toInt, cutOffDateSplit(2).toInt)
    if (localEndDate.isBefore(localCutOffDate)) cellSiteIndices = cellSiteIndex
    else if (localStartDate.isBefore(localCutOffDate) && (localEndDate.isEqual(localCutOffDate) || localEndDate.isAfter(localCutOffDate))) cellSiteIndices = String.join(",", cellSiteIndex, getNonDmatIndex(cellSiteWeeklyIndexDate, endDate, cellSiteIndex))
    else if (localStartDate.isEqual(localCutOffDate) || localStartDate.isAfter(localCutOffDate)) cellSiteIndices = getNonDmatIndex(startDate, endDate, cellSiteIndex)
    //logger.info("Cell site indices calculated: {}", cellSiteIndices)
    cellSiteIndices
  }
  def getNonDmatIndex(startDate: String, endDate: String, indexName: String): String = {
    var indexExpr = ""
    val cal = new GregorianCalendar
    val calEnd = new GregorianCalendar
    val sStartDate = startDate.split("-")
    cal.set(Calendar.DAY_OF_MONTH, sStartDate(2).split("T")(0).toInt)
    cal.set(Calendar.MONTH, sStartDate(1).toInt - 1)
    cal.set(Calendar.YEAR, sStartDate(0).toInt)
    val sEndDate = endDate.split("-")
    calEnd.set(Calendar.DAY_OF_MONTH, sEndDate(2).split("T")(0).toInt)
    calEnd.set(Calendar.MONTH, sEndDate(1).toInt - 1)
    calEnd.set(Calendar.YEAR, sEndDate(0).toInt)
    val startWeek = cal.get(Calendar.WEEK_OF_YEAR)
    val endWeek = calEnd.get(Calendar.WEEK_OF_YEAR)
    var i = 0
    if (endWeek >= startWeek && cal.getWeekYear == calEnd.getWeekYear) {
      i = startWeek
      while ( {
        i <= endWeek
      }) indexExpr = indexExpr + indexName + "-" + cal.getWeekYear + "-" + i + "," {
        i += 1; i - 1
      }
    }
    else {
      i = startWeek
      while ( {
        i <= 52
      }) {
        indexExpr = indexExpr + indexName + "-" + cal.getWeekYear + "-" + i + ","

        {
          i += 1; i - 1
        }
      }
      i = 1
      while ( {
        i <= endWeek
      }) indexExpr = indexExpr + indexName + "-" + calEnd.getWeekYear + "-" + i + "," {
        i += 1; i - 1
      }
    }
    indexExpr.substring(0, indexExpr.length - 1)
  }
  def modifyLogRecordByteArrayToDropHeaderForHBFlex(data: Array[Byte]): Array[Byte] = {
    if (data.length > 5) {
      //parse all nav header information example below for device type remember that endian is flipped from qualcomm!
      val deviceType = data(5)
      //removing the "File Navigation Header" so it is just the payload
      ParseUtils.getSubPayload(data, 6, data.length)
    } else {
      //this doesn't have enough bytes for this logRecord
      data
    }
  }

  def switchEndianOnOffsetAndLength(data: Array[Byte], index: Int, length: Int): Array[Byte] = {
    val swapdata = new Array[Byte](length)
    System.arraycopy(data, index, swapdata, 0, length)
    //println("Get swapdata length:" + swapdata.length)
    for (i <- 0 to length-1) {
      data(index + i) = swapdata(swapdata.length - i - 1)
    }
    data
  }

  def calculateHbflexLogCodeWithRecordLength(data:Array[Byte], recordLenReq:Boolean=false)={
    if(recordLenReq)ParseUtils.GetIntFromBitArray(data,0,16,false) else ParseUtils.GetIntFromBitArray(data,96,16,false)
  }

  def calculateHbflexDmTimestamp(data: Array[Byte]) = {
    var localData = data
    localData = switchEndianOnOffsetAndLength(data, 32/8, 4)
    val timeSeconds = ParseUtils.GetIntFromBitArray(localData, 32, 32, false)
    /*    import java.util.Calendar
        val cal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
        cal.set(1970, 0, 1, 0, 0, 0)
        cal.set(Calendar.SECOND, 0)
        val result = cal.getTimeInMillis / 1000
        val dt = result + new BigInteger(timeSeconds.toString, 16).longValue() + 18*/

    //    var dt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
    //
    //    dt = dt.AddSeconds(new BigInteger(timeSeconds.toString, 16).longValue() + 18).ToLocalTime

    localData = switchEndianOnOffsetAndLength(data, 64 / 8, 4)
    val timeNanos = ParseUtils.GetIntFromBitArray(localData, 64, 32, false)

    var nanosInt = Math.abs(timeNanos)
    while (nanosInt >= 1000) nanosInt /= 10
    var timeNanosStr = timeNanos.toString
    //see if you should round up from the fourth digit of the nano seconds
    if(timeNanosStr!=null && timeNanosStr.startsWith("-")){
      timeNanosStr = "000"
    }
    else{
      if (timeNanos.toString.length > 3) {
        // println("Get time nano substring: " + Integer.parseInt(timeNanos.toString.substring(3, 1)))
        if (Integer.parseInt(timeNanos.toString.substring(3, 4)) >= 5) {
          nanosInt += 1
          timeNanosStr = nanosInt.toString
        }
      } else {
        while (timeNanosStr.length < 3) timeNanosStr = timeNanosStr + "0"
      }
    }

    import java.text.DateFormat
    import java.text.SimpleDateFormat
    val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS")
    df.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

    // val timeMillSecs = new java.sql.Date(new BigInteger((timeSeconds*1000).toString, 16).longValue() + 18)
    val timeMillSecs = new java.sql.Date((timeSeconds + 18)* 1000L)
    //println("Time in Millis......."+timeMillSecs)
   //println("Time NanosStr......."+timeNanosStr)
    timeNanosStr = if(timeNanosStr.length >=3) timeNanosStr.substring(0,3) else timeNanosStr
    df.parse(df.format(timeMillSecs) + timeNanosStr).getTime

  }

  def getFileBaseDir(fileUploadDate: String) = getBaseDrive(getDifference(fileUploadDate))

  def getDifference(fileUploadDate: String): Int = try {
    val fileUploadedDate = new SimpleDateFormat("MMddyyyy").parse(fileUploadDate)
    val fileUploadedDateCalendar = new GregorianCalendar
    fileUploadedDateCalendar.setTime(fileUploadedDate)
    val currentDateCal = new GregorianCalendar
    currentDateCal.setTime(new Date)
    val diffYear = currentDateCal.get(Calendar.YEAR) - fileUploadedDateCalendar.get(Calendar.YEAR)
    diffYear * 12 + (currentDateCal.get(Calendar.MONTH) - fileUploadedDateCalendar.get(Calendar.MONTH))
  } catch {
    case e: ParseException =>
      logger.error("Invalid date format will search only latest file", e)
      0
  }

  def getBaseDrive(number: Int): String = {
    var baseDir = "month%s"
    if (Math.abs(number) <= 6) number match {
      case 0 =>
        baseDir = String.format(baseDir, "0")
      case 1 =>
        baseDir = String.format(baseDir, "1")
      case 2 =>
        baseDir = String.format(baseDir, "2")
      case 3 =>
        baseDir = String.format(baseDir, "3")
      case 4 =>
        baseDir = String.format(baseDir, "4")
      case 5 =>
        baseDir = String.format(baseDir, "5")
      case 6 =>
        baseDir = "backup"
      case _ =>
        baseDir = String.format(baseDir, "0")
    }
    else baseDir = "backup"
    baseDir
  }

  def checkHadoopFileExists(inputPath:String, hdfsURI:String, commonConfigParams: CommonConfigParameters):Boolean = {
    val conf = new Configuration()
    conf.addResource(new Path(commonConfigParams.PATH_TO_CORE_SITE))
    conf.addResource(new Path(commonConfigParams.PATH_TO_HDFS_SITE))
    val fs: FileSystem = FileSystem.get(conf)
    fs.exists(new Path(inputPath))
  }

  def deleteSeqFile(inputPath:String, hdfsURI:String, commonConfigParams: CommonConfigParameters):Boolean = {
    var fileRemoved:Boolean = false
    val conf = new Configuration()
    conf.addResource(new Path(commonConfigParams.PATH_TO_CORE_SITE))
    conf.addResource(new Path(commonConfigParams.PATH_TO_HDFS_SITE))
    val fs: FileSystem = FileSystem.get(conf)
    val filePath:Path = new Path(inputPath)
    if(fs.exists(filePath)){
      fileRemoved = fs.delete(filePath, true)
    }
    fileRemoved
  }

  def parseCellIdtoBinary(inputString:String)={
    var output:String=inputString
    val hexStrArr = inputString.toLowerCase().split(" ")
    if(hexStrArr.length > 3){
      val thirdPos = hexStrArr(2).replace("0x", "")
      val hexCode = "0x" + hexStrArr(0).replace("0x", "") + hexStrArr(1).replace("0x", "") + thirdPos.substring(0, 1)
      val lastVal = Integer.parseInt(thirdPos.substring(1), 16).toBinaryString + hexStrArr.apply(hexStrArr.length - 1).substring(0, 4)
      output = java.lang.Long.decode(hexCode) + "." + Integer.parseInt(lastVal, 2)
    }
    output
  }

  def printHexBinary(input:Array[Byte], formatted:Boolean=false):String={
    var outputStr = ""
    if(input!=null){
      outputStr = javax.xml.bind.DatatypeConverter.printHexBinary(input)
      if(formatted){
        outputStr = outputStr.replaceAll("..","$0 ")
      }
    }
    outputStr
  }

  def emailFormatter(input:String):String = {
    var output = ""
    if(input.nonEmpty){
      val spltArr = input.split("@")
      if(spltArr.length>1){
        output = spltArr(0)+"@"+spltArr(1).toLowerCase
      }
    }
    output
  }

  def getCarrierByFileName(currentFileName:String):String = {
    var carrier = AVAILABLE_CARRIERS(3)
    var fileTypeIdentifier = "dlf"
    if(currentFileName.contains("_dml_dlf.seq")){
      fileTypeIdentifier = "dml_dlf"
    }
    else if(currentFileName.contains("_drm_dlf.seq")){
      fileTypeIdentifier = "drm_dlf"
    }
    else if(currentFileName.contains("_sig.seq")){
      fileTypeIdentifier = "sig"
    }
    /*else if(currentFileName.contains("_tracewin.seq")){
      fileTypeIdentifier = "trace"
    }*/
    fileTypeIdentifier match {
      case "trace" =>
        carrier = CARRIER_TRACE
      case "dlf" =>
      carrier = AVAILABLE_CARRIERS(3)
      case "drm_dlf" =>
        val spltFile = currentFileName.split("_drm_dlf")
        carrier = AVAILABLE_CARRIERS(3)
        if(spltFile.length > 0){
          val innerSplt = spltFile.apply(0).split("_")
          if(innerSplt.length >= 3){
            val carIdentifier = Try(innerSplt.apply(2).substring(2,4)).getOrElse(null)
            if(carIdentifier !=null){
              carrier = DRM_AVAILABLE_CARRIERS(carIdentifier.toUpperCase)
    }
          }
        }
      case "sig" =>
        carrier = CARRIER_SCANNER
      case "dml_dlf" =>
      import scala.util.control.Breaks._
      breakable {
        for (i <- AVAILABLE_CARRIERS) {
          carrier = i
          if (StringUtils.containsIgnoreCase(currentFileName, carrier)) {
            break
          }
        }
      }
    }
    /*if(currentFileName.contains("_dlf.seq") && !currentFileName.contains("_dml_dlf.seq") && !currentFileName.contains("_dml_dlf.seq")){
      carrier = AVAILABLE_CARRIERS(3)
    } else if(currentFileName.contains("_sig.seq")){
      carrier = CARRIER_SCANNER
    } else if (currentFileName.contains("_drm_dlf.seq")){
      val spltFile = currentFileName.split("_drm_dlf")
      carrier = AVAILABLE_CARRIERS(3)
      if(spltFile.length > 0){
        val innerSplt = spltFile.apply(0).split("_")
        if(innerSplt.length >= 3){
          val carIdentifier = Try(innerSplt.apply(2).substring(2,4)).getOrElse(null)
          if(carIdentifier !=null){
            carrier = DRM_AVAILABLE_CARRIERS(carIdentifier.toUpperCase)
          }
        }
      }
    } else {
      import scala.util.control.Breaks._
      breakable {
        for (i <- AVAILABLE_CARRIERS) {
          carrier = i
          if (StringUtils.containsIgnoreCase(currentFileName, carrier)) {
            break
          }
        }
      }
    }*/
    carrier
  }
  def getIpDirection(pkt13:Int):String = {
    var direction:String = "UNKNOWN"
    if(pkt13==1){
      direction = "INCOMING"
    }
    else if(pkt13 == 0){
      direction = "OUTGOING"
    }
    direction
  }
  @throws[IOException]
  def convertZipInputStreamToInputStream(zipStream: ZipInputStream, currentFile:String): InputStream = {
    val BUFFER_SIZE = 8192
    val streamBuilder = new ByteArrayOutputStream
    var is:InputStream = null
    var bytesRead = 0
    val tempBuffer = new Array[Byte](BUFFER_SIZE * 2)
    val entry = zipStream.getNextEntry.asInstanceOf[ZipEntry]
    try {
      while ( {
        (bytesRead = zipStream.read(tempBuffer)).asInstanceOf[Int] != -1
      }) streamBuilder.write(tempBuffer, 0, bytesRead)
      is = new ByteArrayInputStream(streamBuilder.toByteArray)
    } catch {
      case e: IOException =>
        logger.error("Exception Occurred while Converting ZIP File "+currentFile+" into InputStream.. " + e.getMessage)
        e.printStackTrace()
    }
    is
  }
}
