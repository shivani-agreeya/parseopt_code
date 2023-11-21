package com.verizon.oneparser.common

import java.sql.Timestamp
import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Locale, TimeZone}

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.schema.CustomFile
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.List
import scala.util.Try

object Constants extends LazyLogging {

  val MOVED_TO_HDFS:String = "Moved to HDFS"
  val HIVE_INPROGRESS: String = "Hive Inprogress"
  val HIVE_SUCCESS: String = "Hive Success"
  val UNDER_PROCESSING:String = "Under Processing"
  val TEMP_HIVE_SUCCESS: String = "Temp HS"
  val HIVE_FAILURE: String = "Hive Failure"
  val ES_INPROGRESS: String = "ES Inprogress"
  val ES_SUCCESS: String = "ES Success"
  val CONST_COMPLETE: String = "complete"
  val ES_FAILURE: String = "ES Failure"
  val NO_KPI_FOUND: String = "No KPI Found"
  val SEQ_IN_PROGRESS = "Sequence Inprogress"
  val SEQ_DONE = "Sequence Done"
  val CONST_SPACE = " "
  val CONST_DELIMITER = " -> "
  val CONST_COMMA = ","
  val CONST_EMPTY = ""
  val CONST_DLF = "dlf"
  val CONST_DRM_DLF = "drm_dlf"
  val CONST_DRM_DLF_SEQ = "drm_dml.seq"
  val CONST_DML_DLF = "dml_dlf"
  val CONST_DML_DLF_SEQ = "dml_dlf.seq"
  val CONST_SIG_DLF = "sig_dlf"
  val CONST_SPI_DLF = "spi_dlf"
  val CONST_SIG_DLF_SEQ = "sig_dlf.seq"
  val CONST_ZIP = "zip"
  val CONST_SIG = "sig"
  val CONST_IOS_NAME = "ios"
  val CONST_SIG_SEQ = "sig.seq"
  val CONST_TEXT_SIG = "TEXT_SIG"
  val CONST_DRM_EMAIL = "Nielson@vzwdt.com"
  val CONST_DML_EMAIL = "RootMetrics@vzwdt.com"
  val CONST_SIG_EMAIL = "Dataprodm@vzwdt.com"
  val CONST_SPI_EMAIL = "spirent@vzwdt.com"
  val CONST_SPI_DLF_EMAIL = "projectspace@vzwdt.com"
  val CONST_VZKPIS = "vzkpis"

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
  val PG_OPLOGS_TBL_NAME: String = "oplog"
  val PG_LOGS_TBL_NAME: String = "logs"
  val PG_FILE_RETRY_INFO: String = "fileretryinfo"
  val PG_HDFS_WRITE_INFO: String = "hdfswriteinfo"
  val TEST_SEL_QUERY = "(select * from " + PG_OPLOGS_TBL_NAME + " where cast(creation_time as date)='2018-11-19' and lower(state)='moved to hdfs') as " + PG_OPLOGS_TBL_NAME + ""
  val MALFORMED_PKT = "Malformed Packet"

  val CONST_NUMBER = "NUMBER"
  val CONST_FLOAT = "FLOAT"
  val CONST_DOUBLE = "DOUBLE"
  val CONST_TEST_CONFIG = "TEST CONFIG"
  val CONST_TEST_INFO = "TEST INFO"
  val MS_IN_SEC = 1000D
  val Mbps = 1000000
  val CONST_LV_READY = "READY"
  val CONST_ANALYZING_DATA = "Analyzing Data"
  val Percent = 100
  val DEFAULT_KPI_DOUBLE_VAL: Double = -9999999.0
  val DEFAULT_KPI_INTEGER_VAL: Integer = -9999999
  val MaxRTP_PacketLossVal = 10000

  val TEMP_SOL_ENV = "PROD"
  val TEMP_SOL_POSTGRES_CONN_SDE_URL = "jdbc:postgresql://10.20.40.94:5432/dmat_prod?currentSchema=sde&user=postgres&password=postgres123&socketTimeout=5"
  val TEMP_SOL_POSTGRES_DB_USER = "postgres"
  val TEMP_SOL_POSTGRES_DB_PWD = "postgres123"
  val TEMP_SOL_POSTGRES_DRIVER = "org.postgresql.Driver"
  val DATA_SIZE_LIMIT = 5

  val ARROW_SEPARATOR = "->"
  val ACC_REPORT_KPI_MAP: Map[String,String] = Map("NRSBRSRP" -> "brsrp","NRSBRSRQ" -> "brsrq","NRSBSNR" -> "bsinr")
  val CONST_PROD = "PROD"
  val CONST_PROD2 = "PROD2"
  val CONST_PROD3 = "PROD3"

  val CARRIER_ATT = "AT&T"
  val CARRIER_SPRINT = "Sprint"
  val CARRIER_TMOBILE = "T-Mobile"
  val CARRIER_VERIZON = "Verizon"
  val ES_NON_VMAS_ROUTINE = "dmat"
  val ES_VMAS_ROUTINE= "vmas"
  val CARRIER_ID_TYPE_MAP: Map[String,String] = Map("0" -> "Pcc","1" -> "Scc1","2" -> "Scc2","3" -> "Scc3","4" -> "Scc4","5" -> "Scc5","6" -> "Scc6","7" -> "Scc7").withDefaultValue(null)
   val DEVICE_LOGCODES_SIG: List[String] = List("0xB0C0","0xB144","0xB0A4","0xB193","0xB146","0xB177","0xFE30","0xB16A","0xB11D",
     "0x1FFE","0xB179","0xB16C","0x134F","0xB0A0","0xFC63","0xFE73","0xB173","0xB126","0xB183","0xB062","0xB167","0xB064","0xB169",
     "0xB161","0xB14E","0x1362","0xB0C1","0x138E","0xB163","0xB097","0xB116","0x1088","0x1364","0xB060","0xB165","0xB12C","0xFC78",
     "0xB13C","0xB16D","0xB16F","0xB087","0x7021","0x184E","0xB12A","0xFC4B","0xB081","0x1080","0xB139","0xB16B","0xFC3F","0xFC70",
     "0xB067","0x138F","0xB1C9","0xB166","0x1FFB","0xB063","0xB168","0xB14D","0x1361","0xB17E","0xB162","0xB115","0x1363","0xB172",
     "0xB0C2","0xB0EE","0xB0B4","0xB164","0xFC1D","0xB061","0xFC40","0xB111","0xB18A","0xB17C","0xB16E","0xB160","0xB0B0","0xB113")
  val LOGCODES_5G = Array("0xB975","0xB97F","0xB825","0x2501")
  val LOGCODES_4G = Array("0xB193")
  val LOGCODES_GPS = Array("0xFFFF","0x1014","0x1476","0x2101")
  val LOGCODES_TRACE_AUTOTEST = Array("0xFFF4","0xFFF3")
  var LOGCODES_TRACE = Array("0xFFF4")
  var LOGCODES_AUTOTEST = Array("0xFFF3")

  def fetchUpdatedDate(): String = {
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var currentDate: String = dateFormatter.format(new Date())
    //currentDate = "2019-02-18"
    currentDate
  }

  def fetchUpdatedDirDate(): String = {
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    var currentDate: String = dateFormatter.format(new Date())
    //currentDate = "02182019"
    currentDate
  }

  def getTestId(fileName: String): String = {
    "select id, file_name from " + PG_OPLOGS_TBL_NAME + " where file_name = '" + fileName + "' order by creation_time desc limit 1"
  }

  def getQueryBasedOnFileType(recordType:String, limit: String, timeInterval:String, processingEnv:String="DEV", fileType:String="ALL"): String ={
    var query:String = ""
    val selectionStr:String = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology,isinbuilding,model_name, size, reports_hive_tbl_name, events_count, hive_end_time,start_log_time,last_log_time,filetype,ondemand_by,reprocessed_by,carrier_name,compressionstatus,device_os from " + PG_LOGS_TBL_NAME
    var fileTypeConditionStr:String = ""
    if(!fileType.equalsIgnoreCase("all")){
      val cond = if(!fileType.equalsIgnoreCase("dlf")) " not " else ""
      if(!fileType.equalsIgnoreCase("dlf")){
        fileTypeConditionStr = " and lower(filetype) "+ cond +" in ('dlf')"
      }
    }
    val orderByStr = " and lower(processing_server)='" + processingEnv.toLowerCase + "' order by user_priority_time, filetype, creation_time asc limit " + limit

    recordType.toUpperCase match {
      case "REPROCESS" =>
        //query = selectionStr + " where (lower(status)='reprocess' or lower(status)='sequence failed') "+fileTypeConditionStr + orderByStr
        query = selectionStr + " where (lower(status)='reprocess') "+fileTypeConditionStr + orderByStr
      case "SEQUENCE" =>
        query = selectionStr + " where file_location is not null and lower(status)='moved to hdfs' and lower(es_data_status) not in ('complete') " + fileTypeConditionStr + orderByStr
      case "ES_QUERY" =>
        query = selectionStr + " where lower(status)='hive success' and lower(es_data_status) not in ('complete','es inprogress') " + fileTypeConditionStr + orderByStr
      case "SEQ_2_HIVE" =>
        query = selectionStr + " where file_location is not null and lower(status)='sequence done' and lower(es_data_status) not in ('complete','analyzing data','on ftp server','es inprogress') and reports_hive_tbl_name is null" + fileTypeConditionStr + orderByStr
    }
    query
  }

  def getQueryFor(recordType: String, limit: String, timeInterval: String, processingEnv: String = "DEV"): String = {
    var query: String = ""
    recordType.toUpperCase match {
      case "SEQUENCE" =>
        //query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from "+PG_LOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or creation_time >= now() - interval '"+timeInterval+" hours') and file_location is not null and lower(status)='moved to hdfs' and lower(processing_server)='"+processingEnv.toLowerCase+"' order by filetype, creation_time asc limit "+limit
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where file_location is not null and lower(status)='moved to hdfs' and lower(es_data_status) not in ('complete') and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, user_priority_time, creation_time asc limit " + limit
      case "QCOMM" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "QCOMM2" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm2_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "ASN" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology,  model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and asn_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "NAS" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and nas_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "IP" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and ip_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "B192" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and b192_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "B193" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and b193_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "QCOMM5G" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm_5g_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, creation_time asc limit " + limit
      case "ES_QUERY" =>
        //query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from "+PG_LOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or creation_time >= now() - interval '"+timeInterval+" hours') and lower(status)='hive success' and lower(processing_server)='"+processingEnv.toLowerCase+"' order by filetype, creation_time asc limit "+limit
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where lower(status)='hive success' and lower(es_data_status) not in ('complete','es inprogress') and lower(processing_server)='" + processingEnv.toLowerCase + "' order by filetype, user_priority_time,  creation_time asc limit " + limit
      case "HIVE_STATUS_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set status= \ncase \nwhen (lower(qcomm_status)='hive success' and lower(qcomm2_status)='hive success' \nand lower(b192_status)='hive success' and lower(asn_status)='hive success' and lower(nas_status) = 'hive success' and lower(ip_status)='hive success' " +
          "and lower(b193_status)='hive success' and lower(qcomm_5g_status)='hive success') then 'Hive Success'\n\nwhen (lower(qcomm_status)='hive inprogress' or lower(qcomm2_status)='hive inprogress' \nor lower(b192_status)='hive inprogress' or lower(asn_status)='hive inprogress' or lower(nas_status) = 'hive inprogress' or lower(ip_status)='hive inprogress'\nor lower(b193_status)='hive inprogress' " +
          "or lower(b193_status)='hive inprogress' or lower(qcomm_5g_status)='hive inprogress' or qcomm_status is null or qcomm2_status is null or b192_status is null or asn_status is null or nas_status is null or ip_status is null \nor b193_status is null or qcomm_5g_status is null) then 'Hive Inprogress'\n\nELSE 'Hive Failure' END\n\nwhere file_name=? and id=?"
      case "HIVE_STATUS_QUERY_OP_LOGS" =>
        query = "update " + PG_OPLOGS_TBL_NAME + " set updated_time = now(), state = CASE WHEN (select status from " + PG_LOGS_TBL_NAME + " where file_name=? and lower(status)='hive success' order by creation_time desc limit 1)='Hive Success' " +
          "THEN 'Hive Success' ELSE 'Hive Inprogress' END WHERE file_name=?"

      case "STATES_COUNTY_QUERY" =>
        query = "SELECT s.stusps, c.COUNTYNS, c.name, s.VZ_Regions, (case when s.ID_Country=244 then 'USA' else s.VZ_REGIONS end) as country " +
          "FROM counties_wm AS c JOIN states_wm s ON c.STATEFP = s.STATEFP WHERE ST_Intersects(c.shape, st_geometry(' point(' ||?||' '||?||') ', 3857))"

      case "REGIONS_QUERY" =>
        query = "SELECT vz_regions,name FROM vzw_regions_wm WHERE ST_Intersects(shape, st_geometry(' point(' ||?||' '||?||') ', 3857)) and cur_reg = 1"

      case "HIVE_END_TIME_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set hive_end_time = case when lower(status) in ('hive success','es inprogress') then now() ELSE hive_end_time END where file_name = ? and id = ?"

      case "HIVE_START_TIME_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set hive_start_time = case when lower(status) in ('sequence done') then now() ELSE hive_start_time END where file_name = ? and id = ?"

      case "UPDATE_TBL_NAME_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set reports_hive_tbl_name = case when reports_hive_tbl_name is null then ? ELSE reports_hive_tbl_name END where file_name = ? and id = ?"

      case "SEQ_2_HIVE" =>
        //query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from "+PG_LOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or creation_time >= now() - interval '"+timeInterval+" hours') and file_location is not null and lower(status)='sequence done' and lower(processing_server)='"+processingEnv.toLowerCase+"' order by filetype, creation_time asc limit "+limit
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where file_location is not null and lower(status)='sequence done' and lower(es_data_status) not in ('complete','analyzing data','on ftp server','es inprogress') and lower(processing_server)='" + processingEnv.toLowerCase + "' and reports_hive_tbl_name is null order by filetype, user_priority_time, creation_time asc limit " + limit
    }
    query
  }

  def getQueryForRM(recordType: String, limit: String, timeInterval: String, processingEnv: String = "DEV"): String = {
    var query: String = ""
    recordType.toUpperCase match {
      case "SEQUENCE" =>
        //query = "select id, creation_time, updated_time, file_name, file_location from "+PG_OPLOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or creation_time >= now() - interval '"+timeInterval+" hours') and file_location is not null and lower(state)='moved to hdfs' order by creation_time asc limit "+limit
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where file_location is not null and lower(status)='moved to hdfs' and lower(es_data_status) not in ('complete') and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, user_priority_time,  creation_time asc limit " + limit
      case "QCOMM" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "QCOMM2" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm2_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "ASN" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology,  model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and asn_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "NAS" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and nas_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "IP" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and ip_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "B192" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and b192_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "B193" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and b193_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "QCOMM5G" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where (cast(creation_time as date)='" + fetchUpdatedDate + "' or creation_time >= now() - interval '" + timeInterval + " hours') and file_location is not null and (lower(status)='sequence done' or lower(status)='hive inprogress' or lower(status)='hive success') and qcomm_5g_status is null and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit " + limit
      case "ES_QUERY" =>
        //query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from "+PG_LOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or creation_time >= now() - interval '"+timeInterval+" hours') and lower(status)='hive success' and lower(processing_server)='"+processingEnv.toLowerCase+"' and lower(filetype)='dml_dlf' order by filetype, creation_time asc limit "+limit
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where lower(status)='hive success' and lower(es_data_status) not in ('complete','es inprogress') and lower(processing_server)='" + processingEnv.toLowerCase + "' and lower(filetype)='dml_dlf' order by filetype, user_priority_time,  creation_time asc limit " + limit
      case "HIVE_STATUS_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set status= \ncase \nwhen (lower(qcomm_status)='hive success' and lower(qcomm2_status)='hive success' \nand lower(b192_status)='hive success' and lower(asn_status)='hive success' and lower(nas_status) = 'hive success' and lower(ip_status)='hive success' " +
          "and lower(b193_status)='hive success' and lower(qcomm_5g_status)='hive success') then 'Hive Success'\n\nwhen (lower(qcomm_status)='hive inprogress' or lower(qcomm2_status)='hive inprogress' \nor lower(b192_status)='hive inprogress' or lower(asn_status)='hive inprogress' or lower(nas_status) = 'hive inprogress' or lower(ip_status)='hive inprogress'\nor lower(b193_status)='hive inprogress' " +
          "or lower(b193_status)='hive inprogress' or lower(qcomm_5g_status)='hive inprogress' or qcomm_status is null or qcomm2_status is null or b192_status is null or asn_status is null or nas_status is null or ip_status is null \nor b193_status is null or qcomm_5g_status is null) then 'Hive Inprogress'\n\nELSE 'Hive Failure' END\n\nwhere file_name=? and id=?"

      case "STATES_COUNTY_QUERY" =>
        query = "SELECT s.stusps, c.COUNTYNS, c.name, s.VZ_Regions, (case when s.ID_Country=244 then 'USA' else s.VZ_REGIONS end) as country " +
          "FROM counties_wm AS c JOIN states_wm s ON c.STATEFP = s.STATEFP WHERE ST_Intersects(c.shape, st_geometry(' point(' ||?||' '||?||') ', 3857))"

      case "REGIONS_QUERY" =>
        query = "SELECT vz_regions FROM vzw_regions_wm WHERE ST_Intersects(shape, st_geometry(' point(' ||?||' '||?||') ', 3857)) and cur_reg = 1"

      case "HIVE_END_TIME_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set hive_end_time = case when lower(status) in ('hive success','es inprogress') then now() ELSE hive_end_time END where file_name = ? and id = ?"

      case "HIVE_START_TIME_QUERY" =>
        query = "update " + PG_LOGS_TBL_NAME + " set hive_start_time = case when lower(status) in ('sequence done') then now() ELSE hive_start_time END where file_name = ? and id = ?"

      case "SEQ_2_HIVE" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where file_location is not null and lower(status)='sequence done' and lower(es_data_status) not in ('complete','analyzing data','on ftp server','es inprogress') and lower(processing_server)='" + processingEnv.toLowerCase + "' and reports_hive_tbl_name is null and lower(filetype)='dml_dlf' order by filetype, user_priority_time,  creation_time asc limit " + limit

    }
    query
  }

  def getQueryForHiveBkp(recordType: String, limit: String, bkpStartDate: String, bkpEndDate: String): String = {
    var query: String = ""
    recordType.toUpperCase match {
      case "QCOMM" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and qcomm_status is null order by creation_time asc limit " + limit
      case "QCOMM2" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and qcomm2_status is null order by creation_time asc limit " + limit
      case "ASN" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and asn_status is null order by creation_time asc limit " + limit
      case "NAS" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress')  and nas_status is null order by creation_time asc limit " + limit
      case "IP" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and  ip_status is null order by creation_time asc limit " + limit
      case "B192" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and  b192_status is null order by creation_time asc limit " + limit
      case "B193" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and b193_status is null order by creation_time asc limit " + limit
      case "QCOMM5G" =>
        query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, lastname, emailid, mdn, imei, technology, model_name, size, reports_hive_tbl_name, events_count, hive_end_time from " + PG_LOGS_TBL_NAME + " where cast(creation_time as date) >='" + bkpStartDate + "' and cast(creation_time as date)<='" + bkpEndDate + "' and file_location is not null and lower(status) in ('complete','es inprogress','hive inprogress') and qcomm_5g_status is null order by creation_time asc limit " + limit
    }
    query
  }

  def getHiveSuccessQuery(testIds: String) = {
    "select id, creation_time, updated_time, file_name, file_location, dm_user from " + PG_LOGS_TBL_NAME +
      " where lower(status) in ('hive success','es inprogress') and id in (" + testIds.replaceAll("\\[", "").replaceAll("\\]", "") + ")"
  }

  def getHDFSDirPath(dirType: String, createdDate: String, updatedDate: String, hdfsFileDirPath: String): String = {
    var path: String = ""
    if (createdDate != null && createdDate.nonEmpty) {
      if (createdDate != updatedDate) {
        dirType.toUpperCase match {
          case "DLF" =>
            path = hdfsFileDirPath + createdDate + "/processed/"
          case "SEQ" =>
            path = hdfsFileDirPath + createdDate + "/sequence/"
        }
        path
      } else {
        dirType.toUpperCase match {
          case "DLF" =>
            path = hdfsFileDirPath + updatedDate + "/processed/"
          case "SEQ" =>
            path = hdfsFileDirPath + updatedDate + "/sequence/"
        }
        path
      }
    } else if (updatedDate != fetchUpdatedDirDate) {
      dirType.toUpperCase match {
        case "DLF" =>
          path = hdfsFileDirPath + updatedDate + "/processed/"
        case "SEQ" =>
          path = hdfsFileDirPath + updatedDate + "/sequence/"
      }
      path
    } else {
      dirType.toUpperCase match {
        case "DLF" =>
          path = hdfsFileDirPath + fetchUpdatedDirDate + "/processed/"
        case "SEQ" =>
          path = hdfsFileDirPath + fetchUpdatedDirDate + "/sequence/"
      }
      path
    }
  }

  def getDataFrameFromString(spark: SparkSession, fileList: List[AnyRef]): DataFrame = {
    import spark.implicits._
    val fileListAsStr = fileList.mkString(",")
    val filesDS = fileListAsStr.split(",").map(CustomFile(_)).toList.toDF()
    filesDS
  }

  def getSeqFilePath(currentFileName:String,fileLocation:String, updatedDate: String, hdfsPath:String): String ={
    var seqFilePath:String = ""
    val createdDate:String = getDateFromFileLocation(fileLocation)
    seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate,hdfsPath) + currentFileName
    seqFilePath
  }

  def getSeqFilePathFromDlf(currentFileName: String, fileLocation: String, updatedDate: String, hdfsPath: String): String = {
    var seqFilePath: String = ""
    val createdDate: String = getDateFromFileLocation(fileLocation)
    if (currentFileName.contains(Constants.CONST_DRM_DLF) || currentFileName.contains(Constants.CONST_DML_DLF) || currentFileName.contains(Constants.CONST_SIG_DLF)) {
      if (currentFileName.contains(Constants.CONST_DRM_DLF)) {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "_" + Constants.CONST_DRM_DLF + ".seq")
      }
      else if (currentFileName.contains(Constants.CONST_DML_DLF)) {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "_" + Constants.CONST_DML_DLF + ".seq")
      }
      else {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "_" + Constants.CONST_SIG_DLF + ".seq")
      }
    }
    else {
      if (currentFileName.contains(Constants.CONST_SIG)) {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "_" + Constants.CONST_SIG + ".seq")
      }
      else if(currentFileName.contains("."+Constants.CONST_SPI_DLF)) {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "_" + Constants.CONST_SPI_DLF + ".seq")
      }
      else {
        seqFilePath = getHDFSDirPath("SEQ", createdDate, updatedDate, hdfsPath) + currentFileName.replaceAll("\\.[^.]*$", "") + ".seq"
      }
    }
    seqFilePath
  }

  def getDlfFilePathFromLoc(currentFileName: String, fileLocation: String, updatedDate: String, hdfsPath: String): String = {
    logger.info("HDFS Path Check in Constants.scala>>>>>>>>>>>>>>>" + hdfsPath)
    var seqFilePath: String = ""
    val createdDate: String = getDateFromFileLocation(fileLocation)
    seqFilePath = getHDFSDirPath("DLF", createdDate, updatedDate, hdfsPath) + getDlfFromSeq(currentFileName)
    seqFilePath
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
      if(currentFileName.contains("."+Constants.CONST_SIG)){
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_SIG+".seq")
      }
      else if(currentFileName.contains("."+Constants.CONST_SPI_DLF)){
        seqFilePath = currentFileName.replaceAll( "\\.[^.]*$", "_"+Constants.CONST_SPI_DLF+".seq")
      }
      else {
        seqFilePath = currentFileName.replaceAll("\\.[^.]*$", "") + ".seq"
      }
    }
    seqFilePath
  }

  def getDlfFromSeq(currentFileName:String): String ={
    var dlFile:String = ""
    if(currentFileName.contains(Constants.CONST_DRM_DLF) || currentFileName.contains(Constants.CONST_DML_DLF) || currentFileName.contains(Constants.CONST_SIG_DLF)){
      if(currentFileName.contains(Constants.CONST_DRM_DLF)){
        dlFile = currentFileName.replaceAll( "_"+CONST_DRM_DLF+"\\.seq", "."+Constants.CONST_DRM_DLF)
      }
      else if(currentFileName.contains(Constants.CONST_DML_DLF)){
        dlFile = currentFileName.replaceAll( "_"+CONST_DML_DLF+"\\.seq", "."+Constants.CONST_DML_DLF)
      }
      else{
        dlFile = currentFileName.replaceAll("_" + CONST_SIG_DLF + "\\.seq", "." + Constants.CONST_SIG_DLF)
      }
    }
    else {
      if(currentFileName.contains("_sig.seq")){
        dlFile = currentFileName.replaceAll( "_"+CONST_SIG+"\\.seq", "."+Constants.CONST_SIG)
      }
      else if(currentFileName.contains("_spi_dlf.seq")){
        dlFile = currentFileName.replaceAll( "_"+CONST_SPI_DLF+"\\.seq", "."+Constants.CONST_SPI_DLF)
      }
      else {
        dlFile = currentFileName.replaceAll("\\.seq", ".dlf")
      }
    }
    dlFile
  }

  def getDateFromFileLocation(fileLocation: String): String = {
    var date: String = ""
    try {
      if (fileLocation.nonEmpty) {
        val fileLoc_splt = fileLocation.split("/")
        if (fileLoc_splt.length > 0) {
          val len = fileLoc_splt.length;
          var position = len - 1
          date = fileLoc_splt.apply(position)
          if (!isValidNumeric(date, "NUMBER")) {
            position = len - 2
            date = fileLoc_splt.apply(position)
          }
        }
      }
    }
    catch {
      case ge: Exception =>
        logger.error("Exception occurred while getting date from file location : " + ge.getMessage)
    }
    date
  }

  def getDateFromHDFSPath(fileLocation: String): String = {
    var date: String = ""
    try {
      if (fileLocation.nonEmpty) {
        val fileLoc_splt = fileLocation.split("/")
        if (fileLoc_splt.length > 0) {
          val len = fileLoc_splt.length;
          var position = len - 1
          date = fileLoc_splt.apply(position - 2)
        }
      }
    }
    catch {
      case ge: Exception =>
        logger.error("Exception occurred while getting date from getDateFromHDFSPath : " + ge.getMessage)
    }
    date
  }

  def getJsonFromBean(bean: Any) = {
    val gsonBuilder: GsonBuilder = new GsonBuilder()
    gsonBuilder.serializeSpecialFloatingPointValues()
    val gson: Gson = gsonBuilder.create()
    gson.toJson(bean)
  }

  def decimalRounder(input: String, precision: Int) = {
    var output: String = input
    if (input.nonEmpty && input.length > 3) {
      val splt_arr = input.split("\\.")
      if (splt_arr.size > 1) {
        val afterDecimal = splt_arr.apply(1)
        if (afterDecimal.length > precision) {
          output = splt_arr.apply(0) + "." + afterDecimal.substring(0, precision)
        }
      }
    }
    output
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

  def getPGTimeStamp(inputDate: Date) = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS")
    val todayDate = dateFormatter.format(new Date())
    val logTime = if (inputDate != null) dateFormatter.format(inputDate) else todayDate
    new Timestamp(dateFormatter.parse(logTime).getTime)
  }

  case class SequenceFileInfo(fileName:String="",startLogTime:Date=null, endLogTime:Date=null,carrierName:String=CARRIER_VERIZON,failure_info:String=null,es_data_status:String="Under Processing")
  case class StateRegionCounty(stusps:String="",countyns:String="",name:String="",vz_regions:String="",vz_regions1:String="",country:String="")

  def stringToInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  import java.util.TimeZone

  def toUTCTimeStamp(inputTime: Long): Timestamp = {
    val timeAtLocal = inputTime
    val offset = TimeZone.getDefault.getOffset(timeAtLocal)
    val timeAtUTC = new Timestamp(timeAtLocal - offset)
    timeAtUTC
  }

  sealed trait ImplicitType[T]

  object ImplicitType {

    implicit object IntType extends ImplicitType[Int]

    implicit object StringType extends ImplicitType[String]

    implicit object LongType extends ImplicitType[Long]

    implicit object DoubleType extends ImplicitType[Double]

  }

  def toDouble[T: ImplicitType](n: T): Option[Double] = {
    Try(n.toString.toDouble).toOption
  }

  def isValidDate(timeStamp: Date, logCode: Integer, fileName: String): Boolean = {
    var isValidDate = false

    val calendar_Instance_1 = Calendar.getInstance
    calendar_Instance_1.setTime(new Date)
    calendar_Instance_1.add(Calendar.DATE, 1)
    val currentDate = calendar_Instance_1.getTime

    val calendar_Instance_2 = Calendar.getInstance
    calendar_Instance_2.setTime(timeStamp)
    val logcodeYear = calendar_Instance_2.get(Calendar.YEAR)

    /* Workaround to ignore files with wrong date. Example: 1980, 2080, 2081. Logs from Samsung with ~4 hrs offsets (Sometimes,
    Android system time and modem time are different). To avoid any such difference, one day offset is added. Need to be re-visited */

    if ((logcodeYear >= 2016) && (timeStamp.compareTo(currentDate) <= 0)) {
      isValidDate = true
    } else {
      logger.info("Warning: Invalid Date Present!: " + fileName + " - " + logcodeYear + "; LogCode: " + logCode)
    }

    isValidDate
  }

  def isValidDateSdl(timeStamp: Date, oid: Array[Int]): Boolean = {
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
      var oiString = ""

      if (oid != null) {
        var i = 0
        while (i < oid.length) {
          oiString += oid(i)
          i += 1;
          i - 1
        }
      }
      logger.info("Warning: Invalid Date Present!: " + logcodeYear + "; LogCode: " + oiString)
    }
    isValidDate
  }

  def buildInitQuery(currentEnv: String): String = {
    "SELECT * FROM process_param_values where param_code='ONE_PARSER' and lower(key)='" + currentEnv.toLowerCase + "' order by id desc limit 1"
  }

  def CalculateMsFromSfnAndSubFrames5gNr(numerology: String, firstSfn: Int, firstSfnSlot: Int, lastSfnVal: Int, lastSfnSlot: Int): Double = {
    var durationMs = 0.0
    val MAX_SFN_VAL = 1024
    var lastSfn = lastSfnVal

    if (firstSfn > lastSfn || firstSfn == lastSfn && firstSfnSlot > lastSfnSlot) {
      lastSfn += MAX_SFN_VAL
    }

    numerology match {
      case "15kHz" =>
        if (firstSfn == lastSfn) durationMs = lastSfnSlot - firstSfnSlot + 1
        else {
          durationMs = 10 * (lastSfn - firstSfn - 1)
          durationMs = durationMs + (10 - firstSfnSlot) + lastSfnSlot + 1
        }
      case "30kHz" =>
        if (firstSfn == lastSfn) durationMs = 0.5 * (lastSfnSlot - firstSfnSlot + 1)
        else {
          durationMs = 10 * (lastSfn - firstSfn - 1)
          durationMs = durationMs + (0.5 * ((20 - firstSfnSlot) + lastSfnSlot + 1))
        }
      case "60kHz" =>
        if (firstSfn == lastSfn) durationMs = 0.25 * (lastSfnSlot - firstSfnSlot + 1)
        else {
          durationMs = 10 * (lastSfn - firstSfn - 1)
          durationMs = durationMs + (0.25 * ((40 - firstSfnSlot) + lastSfnSlot + 1))
        }
      case "120kHz" =>
        if (firstSfn == lastSfn) durationMs = 0.125 * (lastSfnSlot - firstSfnSlot + 1)
        else {
          durationMs = 10 * (lastSfn - firstSfn - 1)
          durationMs = durationMs + (0.125 * ((80 - firstSfnSlot) + lastSfnSlot + 1))
        }
      case "240kHz" =>
        if (firstSfn == lastSfn) durationMs = 0.0625 * (lastSfnSlot - firstSfnSlot + 1)
        else {
          durationMs = 10 * (lastSfn - firstSfn - 1)
          durationMs = durationMs + (0.0625 * ((160 - firstSfnSlot) + lastSfnSlot + 1))
        }
      case _ =>
    }
    durationMs
  }

  def addSingleQuotes(fileList: List[AnyRef]): String = {
    var outputStr = ""
    outputStr = fileList.mkString("'", "','", "'")
    outputStr
  }

  def getHiveTableIndex(weeklyTblNames: List[String]): String = {
    var outputIndexStr = ""
    val map1 = weeklyTblNames.map(x => x.split("_")).map(x => (x(1), x(2))).groupBy(_._1).mapValues(_.map(_._2))
    val t = map1.toSeq.sortWith(_._1 < _._1)
    if (t.nonEmpty && t.length > 0) {
      outputIndexStr = "_" + t(0)._1 + "_" + t(0)._2.sortWith(_ < _)(0)
    }
    outputIndexStr
  }

  //DEV Config
  /*spark.POSTGRES_CONN_URL jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=dmat_qa&user=postgres&password=postgres123
  spark.POSTGRES_CONN_SDE_URL jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=sde&user=postgres&password=postgres123&socketTimeout=5
  spark.POSTGRES_DRIVER org.postgresql.Driver
  spark.POSTGRES_DB_URL jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=dmat_qa
  spark.POSTGRES_DB_SDE_URL jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=sde
  spark.POSTGRES_DB_USER postgres
    spark.POSTGRES_DB_PWD postgres123
    spark.ES_HOST_PATH 10.20.40.28
  spark.ES_PORT_NUM 9201
  spark.DM_USER_DUMMY 10088
  spark.DM_USER_HEADLESS 10089*/

  //PREPROD Config
  /*spark.POSTGRES_CONN_URL jdbc:postgresql://10.20.40.109:5432/dmat_preprod?currentSchema=dmat&user=postgres&password=postgres123
  spark.POSTGRES_CONN_SDE_URL jdbc:postgresql://10.20.40.109:5432/dmat_preprod?currentSchema=sde&user=postgres&password=postgres123&socketTimeout=5
  spark.POSTGRES_DRIVER org.postgresql.Driver
  spark.POSTGRES_DB_URL jdbc:postgresql://10.20.40.109:5432/dmat_preprod?currentSchema=dmat
  spark.POSTGRES_DB_SDE_URL jdbc:postgresql://10.20.40.109:5432/dmat_preprod?currentSchema=sde
  spark.POSTGRES_DB_USER postgres
    spark.POSTGRES_DB_PWD postgres123
    spark.ES_HOST_PATH 10.20.40.28
  spark.ES_PORT_NUM 9202
  spark.DM_USER_DUMMY 10040
  spark.DM_USER_HEADLESS 10094*/

  //PROD Config
  /*spark.POSTGRES_CONN_URL jdbc:postgresql://10.20.40.94:5432/dmat_prod?currentSchema=dmat&user=postgres&password=postgres123
  spark.POSTGRES_CONN_SDE_URL jdbc:postgresql://10.20.40.94:5432/dmat_prod?currentSchema=sde&user=postgres&password=postgres123&socketTimeout=5
  spark.POSTGRES_DRIVER org.postgresql.Driver
  spark.POSTGRES_DB_URL jdbc:postgresql://10.20.40.94:5432/dmat_prod?currentSchema=dmat
  spark.POSTGRES_DB_SDE_URL jdbc:postgresql://10.20.40.94:5432/dmat_prod?currentSchema=sde
  spark.POSTGRES_DB_USER postgres
    spark.POSTGRES_DB_PWD postgres123
    spark.ES_HOST_PATH 10.20.40.28
  spark.ES_PORT_NUM 9200
  spark.DM_USER_DUMMY 10062*/
  case class LocalTransportAddress(host: String = "10.20.40.28", port: String = "9300", clusterName: String = "pt-dmat-cluster")

  case class PciRequestInfo(pn:Array[Int],bts:Array[String],tl_lat:Double,tl_lon:Double,br_lat:Double,br_lon:Double,index:String,startDate:String="2019-04-01", endDate:String="2019-04-30")

  case class PciInfo(pci: Integer, lat: Double, lon: Double)
  case class GeometryDto(lat:Double, lon:Double, enodeb_sector_id:String=null, sector:Integer=null, band:Integer=null)
  case class gNbidRequestInfo(pn:Integer=0,nrarfcn:Integer=0,cellType:String ="",Lat:Double,Lon:Double,cellSiteIndx:String,startDt:String="2019-04-01",endDt:String="2019-04-30")
  case class gNodeIdInfo(enodebSectorId:String)
}
