package com.verizon.reports.common

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.List
import scala.util.Try

object Constants extends LazyLogging {
  val CONST_DRM_DLF = "drm_dlf"
  val CONST_DML_DLF = "dml_dlf"
  val CONST_SIG_DLF = "sig_dlf"
  val CONST_SIG = "sig"
  val CONST_DLF = "dlf"

  val HIVE_DB = "dmat_logs."
  val TBL_QCOMM_NAME = "LogRecord_QComm_Ext"
  val TBL_QCOMM2_NAME = "LogRecord_QComm2_Ext"
  val TBL_QCOMM2DUP_NAME = "LogRecord_QComm2Dup_Ext"
  val TBL_B192_NAME = "LogRecord_B192_Ext"
  val TBL_B193_NAME = "LogRecord_B193_Ext"
  val TBL_ASN_NAME = "LogRecord_ASN_Ext"
  val TBL_NAS_NAME = "LogRecord_NAS_Ext"
  val TBL_IP_NAME = "LogRecord_IP_Ext"
  val TBL_QCOMM5G_NAME = "LogRecord_QComm_5G_Ext"
  val PG_LOGS_TBL_NAME:String = "logs"

  val MS_IN_SEC = 1000D
  val Mbps = 1000000D

  val DEFAULT_KPI_DOUBLE_VAL: Double= -9999999.0
  val DEFAULT_KPI_INTEGER_VAL: Integer= -9999999

  def fetchUpdatedDirDate():String={
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    var currentDate: String = dateFormatter.format(new Date())
    currentDate
  }

  def getSeqFromDlf(currentFileName:String): String ={
    var seqFilePath:String = ""
    if(currentFileName.contains(Constants.CONST_DRM_DLF) || currentFileName.contains(Constants.CONST_DML_DLF) || currentFileName.contains(Constants.CONST_SIG_DLF)){
      if(currentFileName.contains(Constants.CONST_DRM_DLF)){
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_DRM_DLF+".seq")
      }
      else if(currentFileName.contains(Constants.CONST_DML_DLF)){
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_DML_DLF+".seq")
      }
      else{
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_SIG_DLF+".seq")
      }
    }
    else {
      if(currentFileName.contains(Constants.CONST_SIG)){
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_SIG+".seq")
      }
      else {
        seqFilePath = currentFileName.replaceAll("\\.[^.]*$", "") + ".seq"
      }
    }
    seqFilePath
  }

  def getDateFromFileLocation(fileLocation:String):String={
    var date:String = ""
    try {
      if (fileLocation.nonEmpty) {
        val fileLoc_splt = fileLocation.split("/")
        if (fileLoc_splt.length > 0) {
          val len = fileLoc_splt.length;
          var position = len-1
          date = fileLoc_splt.apply(position)
          if(!isValidNumeric(date,"NUMBER")){
            position = len-2
            date = fileLoc_splt.apply(position)
          }
        }
      }
    }
    catch{
      case ge:Exception =>
        logger.error("Exception occurred while getting date from file location : "+ge.getMessage)
    }
    date
  }

  def isValidNumeric(input:String, dataType:String):Boolean = {
    var isValid:Boolean = false
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

  def addSingleQuotes(fileList: List[AnyRef]): String = {
    var outputStr = ""
    outputStr = fileList.mkString("'", "','", "'")
    outputStr
  }
}
