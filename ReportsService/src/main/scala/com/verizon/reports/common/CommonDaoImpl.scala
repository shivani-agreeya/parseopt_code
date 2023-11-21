package com.verizon.reports.common

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement, Types}
import java.text.SimpleDateFormat
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object CommonDaoImpl extends LazyLogging{
  import java.util.{Calendar, TimeZone}
  val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  def  getVariableRangesForKpi(spark: SparkSession,kpiName: String, commonConfigParams:CommonConfigParameters): Dataset[(Double,Double)]={
    var varRangeDf:Dataset[Row] = null
    var conn: Connection = null
    var varRangeDs:Dataset[(Double,Double)] = null

    //logger.info("Connection String for : "+Constants.CURRENT_ENVIRONMENT+ " --> "+commonConfigParams.POSTGRES_DB_URL)
    val query="select min,max from prepostkpirange where kpikey=?"
    logger.info("Query for Logs Tables>>>>>"+query)
    val varRangeList = mutable.ListBuffer.empty[(Double, Double)]
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection(commonConfigParams.POSTGRES_DB_URL, getProperties(commonConfigParams))
      val varRangeQuery = conn.prepareStatement(query)
      varRangeQuery.setString(1, kpiName)
      val varRangRs = varRangeQuery.executeQuery()
      while(varRangRs.next()){
        varRangeList+=((varRangRs.getDouble("min"),varRangRs.getDouble("max")))
      }
      import spark.implicits._
      varRangeDs=varRangeList.toDS()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      conn.close()
    }
    varRangeDs
  }
   def  getFixedRangesForKpi(spark: SparkSession,kpiName: String, commonConfigParams:CommonConfigParameters): Dataset[String]={
    var varRangeDf:Dataset[Row] = null
    var conn: Connection = null
    var varRangeDs:Dataset[String] = null

    //logger.info("Connection String for : "+Constants.CURRENT_ENVIRONMENT+ " --> "+commonConfigParams.POSTGRES_DB_URL)
    val query="select value from prepostkpirange where kpikey=?"
    logger.info("Query for Logs Tables>>>>>"+query)
    val varRangeList = mutable.ListBuffer.empty[String]
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection(commonConfigParams.POSTGRES_DB_URL, getProperties(commonConfigParams))
      val varRangeQuery = conn.prepareStatement(query)
      varRangeQuery.setString(1, kpiName)
      val varRangRs = varRangeQuery.executeQuery()
      while(varRangRs.next()){
        varRangeList+=((varRangRs.getString("value")))
      }
      import spark.implicits._
      varRangeDs=varRangeList.toDS()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      conn.close()
    }
    varRangeDs
  }

   def getAllCoverageKpisByType(commonConfigParams:CommonConfigParameters) ={
    var connection:Connection =null
    var pst: PreparedStatement= null
    import scala.collection.mutable.Map
    var retVal: Map[String,String]=Map()
    val query: String = "select distinct kpikey,case when p.min is not null then 'variable' else 'fixed' end from prepostkpirange p"
    try{
      connection = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)
      pst=connection.prepareStatement(query)
      val rs=pst.executeQuery()
      while(rs.next()){
        retVal+=(rs.getString(1) -> rs.getString(2))
      }

    }catch{
      case e:Throwable =>
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
    } finally {

      pst.close
      connection.close
    }
    retVal
  }

  def insertPdfCdfReportData(kpiName: String,kpiPfCdfs: Array[(String,Double,Double,Long)],processingType: String, reportId: Long,reportprefix: String,count: Long,commonConfigParams:CommonConfigParameters): Unit={
    logger.info("Inserting into report_job and report_kpi_data")

    val connection:Connection = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)

    val reportDataSql="insert into report_kpi_data (jobid,kpi_name,process_type,var_kpi_range,pdf,cdf,report_name,count) values (?,?,?,?,?,?,?,?)"
    val psBatch= connection.prepareStatement(reportDataSql)
    try {
      kpiPfCdfs.foreach(value => {
        psBatch.setLong(1,reportId)
        psBatch.setString(2,kpiName)
        psBatch.setString(3,processingType)
        psBatch.setString(4,value._1)
        psBatch.setDouble(5,value._2)
        psBatch.setDouble(6,value._3)
        psBatch.setString(7,reportprefix +kpiName)
        psBatch.setLong(8,value._4)
        psBatch.addBatch()
      })
      psBatch.executeBatch()
    } catch{
      case e:Throwable =>
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
    } finally {

      psBatch.close
      connection.close
    }
  }

  //query = "select id, creation_time, updated_time, file_name, file_location, dm_user, firstname, 
  //lastname, emailid, mdn, model_name from "+PG_LOGS_TBL_NAME+" where (cast(creation_time as date)='"+fetchUpdatedDate+"' or
  //creation_time >= now() - interval '"+timeInterval+" hours') 
  //and lower(status)='hive success' order by creation_time asc limit "+limit
  def updateJobStatus(jobRepId: Long,jobStatus: String, reason: String, commonConfigParams:CommonConfigParameters): Unit ={
    //val sql = "insert into logfile_report (pre_filename,post_filename,submit_daterequested,dm_user,report_submit_status,
    //pre_from_date,pre_to_date,post_from_date,post_to_date) values " +
    val query="update logfile_report set report_submit_status =? , failure_details = ? where job_id=?"
    var conn: Connection = null
    var logsQuery:PreparedStatement = null
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection(commonConfigParams.POSTGRES_DB_URL, getProperties(commonConfigParams))
      logsQuery = conn.prepareStatement(query)
      logsQuery.setString(1, jobStatus)
      if(reason!=null){
        logsQuery.setString(2, reason)
      }else{
        logsQuery.setNull(2, Types.VARCHAR)
      }
      logsQuery.setLong(3, jobRepId)
      logsQuery.executeUpdate
    }catch{
      case e: Exception => e.printStackTrace()
    }finally{
      try{
        conn.close()
        logsQuery.close()
      }catch{
        case e: Exception => e.printStackTrace()
      }
    }
  }
 def getLogsDataForReports(spark:SparkSession,commonConfigParams:CommonConfigParameters,
     fileName:String,fileCaptureDate: String,testId: Long):Dataset[Row] = {

   val query=s"""select fl.id, fl.creation_time, fl.updated_time, fl.file_name, fl.file_location, fl.dm_user, fl.firstname,fl.lastname, fl.emailid,
    fl.mdn,fl.imei,fl.technology, fl.model_name,fl.reports_hive_tbl_name from logs as fl where  lower(fl.es_data_status) ='complete' 
    and fl.file_name= '$fileName'  and fl.id=$testId  """

    val logsTblList = mutable.ListBuffer.empty[(String, Int, String, String,String, Int, String, String, String,String,String,String,String,String,String)]
    val outSdf:SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    val hiveUpSdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val inSdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var conn: Connection = null
    var logTblDs:Dataset[Row] = null
    //logger.info("Connection String for : "+Constants.CURRENT_ENVIRONMENT+ " --> "+commonConfigParams.POSTGRES_DB_URL)
    logger.info("Query for Logs Tables>>>>>"+query)
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection(commonConfigParams.POSTGRES_DB_URL, getProperties(commonConfigParams))
      val logsQuery = conn.prepareStatement(query)
      val logsRows = logsQuery.executeQuery()
      while (logsRows.next()) {
        val fileName = logsRows.getString("file_name")
        val testId = logsRows.getInt("id")
        val updatedDate = logsRows.getString("updated_time")
        val createdDate = logsRows.getString("creation_time")
        val fileLocation = logsRows.getString("file_location")
        val DmUser = logsRows.getInt("dm_user")
        val first_Name = logsRows.getString("firstname")
        val last_Name = logsRows.getString("lastname")
        val emailID = logsRows.getString("emailid")
        val mdn_logs = logsRows.getString("mdn")
        val imei_logs = logsRows.getString("imei")
        val technology_logs = logsRows.getString("technology")
        val modelName_logs = logsRows.getString("model_name")
        val hiveUpdatedDate = logsRows.getString("updated_time")
        val hiveTableVersion: String = logsRows.getString("reports_hive_tbl_name")
        logsTblList += ((fileName, testId, outSdf.format(inSdf.parse(updatedDate)), outSdf.format(inSdf.parse(createdDate)),fileLocation,
            DmUser, first_Name, last_Name, emailID, mdn_logs,
            imei_logs,technology_logs, modelName_logs,
            hiveUpSdf.format(inSdf.parse(hiveUpdatedDate)),hiveTableVersion))
      }
      import spark.implicits._
      logTblDs = logsTblList.
      toDF("fileName", "testId", "updatedDate", "createdDate","fileLocation", "DmUser", "first_Name", "last_Name", "emailID","mdn_logs","imei_logs",
          "technology_logs","modelName_logs","hiveUpdatedDate","hiveTableVersion")

    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      conn.close()
    }

    logTblDs
  }

 def insertReportJobDetails(commonConfigParams:CommonConfigParameters): Int ={
    logger.info("Inserting into report_job and report_kpi_data")
    val reportJobSql = "insert into report_job(status,email) values ('success','gopi@gmail.com')"
    val connection:Connection = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)
    val ps: PreparedStatement = connection.prepareStatement(reportJobSql,Statement.RETURN_GENERATED_KEYS)
    var jobId: Int= -1
    try {
      ps.executeUpdate()
      val rs: ResultSet = ps.getGeneratedKeys();
      if (rs.next()) {
        jobId = rs.getInt(1);
      }

    } catch{
      case e:Throwable =>
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
    } finally {
      ps.close
      connection.close
    }
    jobId;
  }

  def getProperties(commonConfigParams:CommonConfigParameters)={
    val prop = new Properties()
    prop.setProperty("user", commonConfigParams.POSTGRES_DB_USER)
    prop.setProperty("password", commonConfigParams.POSTGRES_DB_PWD)
    prop.setProperty("driver", commonConfigParams.POSTGRES_DRIVER)
    prop
  }
  
  def saveCongestionData(aggData: Array[Row],xAxisKpiName: String,averagedKpiName: String,
      jobId: Long,processType: String,reportName: String,commonConfigParams: CommonConfigParameters) ={
      val query = "insert into report_kpi_data (jobid,process_type,report_name,kpi_name,groupbyname,fixed_kpi_val,kpi_avg_val) values (?,?,?,?,?,?,?)"
      var connection:Connection =null
      
      var psBatch:PreparedStatement =null

    try {
      connection = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)
       psBatch= connection.prepareStatement(query)
      aggData.foreach(value => {
        psBatch.setLong(1,jobId)
        psBatch.setString(2,processType)
        psBatch.setString(3,reportName)
        psBatch.setString(4,averagedKpiName)
        psBatch.setString(5,xAxisKpiName)
        psBatch.setString(6,value.get(1).toString())
        psBatch.setDouble(7,value.getDouble(0))
        
        psBatch.addBatch()
      })
      psBatch.executeBatch()
    } catch{
      case e:Throwable =>
        logger.error("Exception Occured while inserting into logs table:"+e.getMessage)
    } finally {

      psBatch.close
      if(connection!=null)
      connection.close
    }

  }

  def getCongestionChartData(commonConfigParams:CommonConfigParameters):scala.collection.immutable.Map[String,scala.collection.immutable.List[CongestionData]] = {

    var congestionChartMap: scala.collection.immutable.Map[String,scala.collection.immutable.List[CongestionData]] = null
    var dbc: Connection = null
    var st: PreparedStatement = null;
    try{
    dbc = DriverManager.getConnection(commonConfigParams.POSTGRES_CONN_URL)
    val sql = "select group_kpi_key,avg_kpi_key,norm_max_val_flag,reportname,grp_key_def_val,avg_key_def_val from prepost_congestion_chart_mst"
    st = dbc.prepareStatement(sql)
    val rs:ResultSet =st.executeQuery()
    var congestionList: ListBuffer[CongestionData]=ListBuffer()
    while(rs.next()){
      congestionList+=CongestionData(groupKpiKey=rs.getString("group_kpi_key"),avgKpiKey=rs.getString("avg_kpi_key"),
          normMaxValFlag=rs.getBoolean("norm_max_val_flag"),
          reportName=rs.getString("reportname"),grpDefVal=rs.getString("grp_key_def_val"),avgDefVal=rs.getString("avg_key_def_val"))
    }
    import scala.collection.immutable.Map
    val congestionMap:Map[String, ListBuffer[CongestionData]]=congestionList.groupBy(congestionKey => congestionKey.reportName)

    congestionChartMap=congestionMap.map(mapEntry => (mapEntry._1,collection.immutable.List(mapEntry._2: _*)))
     } catch{
      case e:Throwable =>
        logger.error("Exception Occured while inserting into logs table:",e)
    } finally {
      if(st!=null)
      st.close
      if(dbc!=null)
      dbc.close
    }
    return congestionChartMap
  }
}

case class UserInfo(user_id:Integer=0, firstName:String="", lastName:String="", email:String="")
case class LogFileDto(testId:Integer=0, fileName:String="", fileLocation:String="", compressionStatus:Boolean=false, status:String="", esDataStatus:String="",failureInfo:String="")
case class ProcessParams(id:Integer=0, recordType:String="", struct_chng:String="", cnt_chk:String="")
case class CongestionData(groupKpiKey: String, avgKpiKey: String, normMaxValFlag: Boolean, reportName: String, grpDefVal: String, avgDefVal: String)
