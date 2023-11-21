package com.verizon.reports

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.typesafe.scalalogging.{LazyLogging, Logger}
import com.verizon.reports.common.{CommonConfigParameters, CommonDaoImpl, CongestionData, Constants}
import com.verizon.reports.dto.GetInputReportDto
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

import scala.collection.immutable._
import scala.collection.mutable.Map
import scala.util.Try
import org.apache.commons.lang.time.DateUtils

@Component
class ProcessReportRecords extends Serializable with LazyLogging {

  @transient
  protected override lazy val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
  //TODO remove the auto injection and create one spark session per request submission
  @Autowired
  private val sparkSession: SparkSession = null
  @Autowired
  @transient
  private val environment: Environment = null

  @Autowired
  private val commonConfigParams: CommonConfigParameters = null




  def processReportData(dto: GetInputReportDto, jobRepId: Long): Unit = {
    
    val jobId = jobRepId
    
    logger.info("processReportData invoked from controller!!!!!!! Pre: " + dto.prefileName + "  Post:   " + dto.postfileName)
    val spark = SparkSession.builder().enableHiveSupport().config("spark.submit.deployMode", "client").getOrCreate()
    
    val conf = spark.sparkContext.getConf
    val reportCutoffDateStr=environment.getProperty("spark.REPORT_LOG_CUTOFF_DATE")
    conf.setAppName("ReportView")
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "10000000")
    logger.info("Job started !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+jobId)
    logger.info("TBL_QCOMM_NAME="+spark.conf.get("spark.TBL_QCOMM_NAME"))
    logger.info("TBL_QCOMM2_NAME="+spark.conf.get("spark.TBL_QCOMM2_NAME"))
    logger.info("TBL_B192_NAME="+spark.conf.get("spark.TBL_B192_NAME"))
    logger.info("TBL_B193_NAME="+spark.conf.get("spark.TBL_B193_NAME"))
    logger.info("TBL_ASN_NAME="+spark.conf.get("spark.TBL_ASN_NAME"))
    logger.info("TBL_NAS_NAME="+spark.conf.get("spark.TBL_NAS_NAME"))
    logger.info("TBL_IP_NAME="+spark.conf.get("spark.TBL_IP_NAME"))
    logger.info("TBL_QCOMM5G_NAME="+spark.conf.get("spark.TBL_QCOMM5G_NAME"))
    logger.info("spark.REPORT_LOG_CUTOFF_DATE="+reportCutoffDateStr)
    val processStartTime = System.currentTimeMillis
    logger.info("request is >>>>>>>>>>>>>>>>>>>>>>>>> ####### "+dto.toString())
    val jobStatus: Boolean=
    if (dto.prefileName != null && dto.postfileName != null) {
      logger.info("pre file name "+dto.prefileName +"   >>>>>> test id  >> "+dto.preFileTestId)
      logger.info("TBL_QCOMM5G_NAME="+spark.conf.get("spark.TBL_QCOMM5G_NAME"))
      val preJobStaus = processReportRecords(spark, dto.prefileName, "pre", new Date().toString(), jobId, 
          commonConfigParams,dto.preTimeFrom,dto.preFileTestId)
      if (preJobStaus) {
        logger.info("Pre job Success!!!!!!!!!!! " + dto.prefileName)
        logger.info("pre file name "+dto.postfileName +"   >>>>>> test id  >> "+dto.postFileTestId)
        val postJobStaus = processReportRecords(spark, dto.postfileName, "post", new Date().toString(), jobId,
            commonConfigParams,dto.postTimeFrom,dto.postFileTestId)
        if (postJobStaus) {
          logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<******Pre and Post job Success JOBID >>>>"+ jobId + dto.prefileName + "  |    " + dto.postfileName + "******>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          CommonDaoImpl.updateJobStatus(jobId, "job_completed", null, commonConfigParams)
        }else{
          logger.info("post job failed " + dto.postfileName+"JOBID>>>>> "+jobId)
          false
        }
      } else {
        logger.info("Pre job failed " + dto.prefileName+"JOBID>>>>> "+jobId)
        false
      }
      //TODO uncomment this when Samir's service is ready
      
      true
    } else {
      throw new IllegalArgumentException("pre or post files are not submitted properly")
      false
    }
    val processEndTime = System.currentTimeMillis
    val responseString: String =if(jobStatus) initiateReportGeneration(jobId)else  null
    val reportGenerationStatus=if(responseString!=null)responseString.contains("FTP successfully") else false
    val jobStatusStr: String=if(jobStatus && reportGenerationStatus){
      "report generated successfully and emailed" 
    }else if(jobStatus && !reportGenerationStatus) {
      
      " report data generated but failed to fill and email the report "
    }else{
      "report job failed"
    }
    
    
    val totalTimeTaken=    processEndTime - processStartTime
    logger.info(s"""<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<****** prePost job |  $jobId   | <<< $jobStatusStr >>>  |      ${dto.prefileName}
        |    ${dto.postfileName}  |    ******>>>>>>>>>>>>>>>>>>>>>>>>>>>>>$totalTimeTaken """)
    
    
    logger.info(s""">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><Job>  $jobId took  $totalTimeTaken milliseconds <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<  """)    
  }
  def initiateReportGeneration(jobId: Long): String = {
    var result: String = null
    try {

      val url = environment.getProperty("prepost.report.url") + jobId 
      result = scala.io.Source.fromURL(url).mkString
      logger.info("report generated invocation status>>>>>>>>>>>>>>>>>>" + result)
    } catch {
      case e: Exception => logger.error("exception while initiating report ", e)
    }
    result
  }
  def processReportRecords(spark: SparkSession, processFileNames: String, processType: String,
                           processDate: String, jobId: Long,
                           commonConfigParams: CommonConfigParameters,fileCreationDate: String,testId: String): Boolean = {

    var status = true
    val conf = spark.sparkContext.getConf
    logger.info("report data processing started for file " + processFileNames + " process is " + processType)
    //var mapRddRet:Dataset[Map[String,Any]] =null
    implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val dateFormatter: SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
    val currentDate: String = dateFormatter.format(new Date())

    val startTimeRead = System.currentTimeMillis
    logger.info("Started reading file ==>> " + startTimeRead)
    val limit = spark.conf.get("spark.esLimit")
    val sleepTime = spark.conf.get("spark.esSleepTime")
    val timeInterval = spark.conf.get("spark.timeInterval")
    val repartitionNo = spark.conf.get("spark.repartition")

    import org.apache.spark.sql.functions._

    var reportDataRdd: Dataset[ReportData] = null
    var mapRdd: Dataset[Map[String, Any]]=null

    import spark.implicits._


    // while (true) {
    try {
      val renameDlf: (String) => String = (data: String) => {
        Constants.getSeqFromDlf(data)
      }
      
      val processFileName: String = processFileNames
      val renameCol = udf(renameDlf)
      val testIdNum: Long=toLong(testId).getOrElse(-1L)
      if(testIdNum.equals(-1L)) throw new ReportDataException("Invalid testid "+testId)
      val logsTblDsTemp = CommonDaoImpl.getLogsDataForReports(spark, commonConfigParams,processFileName,fileCreationDate,testIdNum)
      
      val logsTblDs = logsTblDsTemp.withColumn("fileName", renameCol(logsTblDsTemp.col("fileName")))//.filter($"fileName" === processFileNameSeq)
      var tableSuffix: String= null;
      var logCreationTime: String=null;
      if (logsTblDs == null || logsTblDs.count == 0) {
        throw new ReportDataException("failure to process file name " + processFileName + " in " + processType + " phase. file status in logs table is incorrect ")
      }else{
        //logsTblDsTemp.show
        val logRow=logsTblDs.take(1);
        tableSuffix=logRow(0).getString(14)
        logCreationTime=logRow(0).getString(2)
        if(tableSuffix==null || logCreationTime==null) throw new ReportDataException("failure to process file name " + processFileName + " in " + processType + " phase. The hive table does not exist ")
      }
      var filesList: List[Any] = List()
      val fileNamesList = logsTblDs.select("fileName").rdd.map(row => row(0).toString).collect().toList
      

      val dateList = logsTblDs.select("fileLocation").rdd.map(row => Constants.getDateFromFileLocation(row(0).toString)).collect().toList.distinct
      val fileNamesStr: String = Constants.addSingleQuotes(fileNamesList)
      val datesStr: String = Constants.addSingleQuotes(dateList) + ",'" + Constants.fetchUpdatedDirDate + "'" //"'05082019'"
      val testIds = logsTblDs.select("testId").rdd.map(row => row(0).toString).collect().toList
      val testIdStr = Constants.addSingleQuotes(testIds)
      val reportCutoffDateStr=environment.getProperty("spark.REPORT_LOG_CUTOFF_DATE")
      var startTimePartition = System.currentTimeMillis
      logger.info("START Partioning dataframe ==>> " + startTimePartition)
      var reportDataRdd: Dataset[Row]=null
      logger.info(s"log creation date fetched from database is $logCreationTime")
      //logCreationTime=logCreationTime.substring(0, logCreationTime.indexOf(" "))
      //val outSdf:SimpleDateFormat = new SimpleDateFormat("MMddyyyy")
      val logCreationDate:Date=DateUtils.parseDate(logCreationTime, Array("MMddyyyy"))
      val logCutOffDate: Date=DateUtils.parseDate(reportCutoffDateStr, Array("yyyy-MM-dd"))
      logger.info(s""" log creation date $logCreationDate and log cut-off date  $logCutOffDate """)
      
      if(logCreationDate.before(logCutOffDate))
      {
        logger.info(s"In side multi table block@@@@@@@@@@@@@@@@@@@@@@@@@*************************** ==>>$tableSuffix ")
        reportDataRdd=ReportDataParser.parseLogData(spark, jobId, fileNamesStr, processType,testIdNum,tableSuffix)
        logger.info(s"ENDED multi table block@@@@@@@@@@@@@@@@@@@@@@@@@*************************** ==>>$tableSuffix ")
      }else{
        logger.info(s"In side SINGLE table block@@@@@@@@@@@@@@@@@@@@@@@@@*************************** ==>>$tableSuffix ")
        reportDataRdd=ReportDataParserSingleTable.parseLogData(spark, jobId, fileNamesStr, processType,testIdNum,tableSuffix)
        logger.info(s"ENDED SINGLE table block@@@@@@@@@@@@@@@@@@@@@@@@@*************************** ==>>$tableSuffix ")
      }
      val timeStart = System.currentTimeMillis()
      //reportDataRdd.show()
      logger.info("ReportDataParser.parseLogData   time taken is >>>>> jobid "+jobId + (System.currentTimeMillis() - timeStart))
      //reportDataRdd.explain()
      logger.info("====================================================================================")
      reportDataRdd.cache()
      reportDataRdd.createOrReplaceTempView("report")
      

      val reportId = jobId

      val reportIdB = spark.sparkContext.broadcast(reportId)

      //MAP TAB start
      import scala.collection.mutable.Map
      val kpiTypeMap: Map[String, String] = CommonDaoImpl.getAllCoverageKpisByType(commonConfigParams)
      kpiTypeMap.foreach(x => {
        if (x._2.equals("variable")) {
          val ranges = CommonDaoImpl.getVariableRangesForKpi(spark, x._1, commonConfigParams)
          PrePostAggegation.savePrepostRangeVariableDataToDb(spark, x._1, processType, reportId, ranges, "distribution_", commonConfigParams)
        } else {
          val ranges = CommonDaoImpl.getFixedRangesForKpi(spark, x._1, commonConfigParams)
          if(!"LTEPccPci".equals(x._1)){
          PrePostAggegation.savePrepostFixedRangeDataToDb(spark, x._1, processType, reportId, ranges, "distribution_", commonConfigParams)
          }
        }
      })
      //MAP TAB END

      //CELL ID START

      val aggReportData = spark.sql("select lteCellID,count(lteCellID) cellid_count,min(if(ltePccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrp)  ) minrsrp,max(if(ltePccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrp)) maxrsrp," +
        "avg(if(ltePccRsrp=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrp)) avgrsrp,min(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrq)) minrsrq,max(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrq)) maxrsrq,avg(if(ltePccRsrq=" + Constants.DEFAULT_KPI_DOUBLE_VAL + ",0.0,ltePccRsrq)) avgrsrq from report " +
        " where lteCellID!=" + Constants.DEFAULT_KPI_INTEGER_VAL + " and (ltePccRsrp!=" + Constants.DEFAULT_KPI_DOUBLE_VAL + " or ltePccRsrq!=" + Constants.DEFAULT_KPI_DOUBLE_VAL + " ) group by lteCellID")

      implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
      // val reportIdB=spark.sparkContext.broadcast(reportId)
      logger.info("=================================================================fetching cell id count from below RDD!!!!!!!!!")
      logger.info("=================================================================fetching cell id count!!!!!!!!!")
      if (aggReportData.count > 0) {
        val binCount = aggReportData.select(col("cellid_count")).rdd.map(_(0).asInstanceOf[Long]).reduce(_ + _)
        val binCountBc = spark.sparkContext.broadcast(binCount)
        val lteRadiAccReport = aggReportData.flatMap((row) => {
          val lteCellID = row.getInt(0)
          val cellCount = row.getLong(1)
          val minRsrp = row.getDouble(2)
          val maxRsrp = row.getDouble(3)
          val avgRsrp = row.getDouble(4)

          val minRsrq = row.getDouble(5)
          val maxRsrq = row.getDouble(6)
          val avgRsrq = row.getDouble(7)
          val binPercent = if (binCountBc.value == 0.0) 0.0 else (cellCount * 100.0) / binCountBc.value

          List(
            LteRaReport(reportIdB.value, processType, null, null, "ltePccRsrp", 0.0, 0.0, cellCount, 0.0, minRsrp, maxRsrp, avgRsrp, lteCellID.toString, "CellIdReport", binPercent, 0.0),
            LteRaReport(reportIdB.value, processType, null, null, "ltePccRsrq", 0.0, 0.0, cellCount, 0.0, minRsrq, maxRsrq, avgRsrq, lteCellID.toString, "CellIdReport", binPercent, 0.0))
        })
        if (lteRadiAccReport.count > 0) {
          lteRadiAccReport.write.mode(SaveMode.Append).jdbc(commonConfigParams.POSTGRES_CONN_URL, "report_kpi_data", getConnectionProperties(commonConfigParams))
        }
      }

      logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>END of RDD>>>>>>>>>>>>>>>>>>")

      logger.info("Unpersisted both dmatQualCommWithLoc and qualCommDf>>>>>")

      logger.info("REPORT>>>>>>>>>>>>>>>>>>>>>>>>>>>>>end report>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      logger.info("Unpersisted both dmatQualCommWithLoc and qualCommDf>>>>>")

      //UE COVERAGE
      logger.info("UE COVERAGE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>UE COVERAGE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      // PrePostAggegation.SaveUECoverage(spark,jobId,processType)

      if (PrePostAggegation.SaveUECoverage(spark, jobId, processType, commonConfigParams) == false) {
        logger.info("NO UE COVERAGE FOUND FOR GIVEN EARNFCN.....>>>>>>>>>>>>>>>>>>>>>>>>>>>>>end report>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      } else
        logger.info("UE COVERAGE END>>>>>>>>>>>>>>>>>>>>>>>>>>>>>end report>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      logger.info("RCCC CONNECTIION START....>>>>>>>>>>>>>>>>>>>>>>>>>>>>> report>>>>>>>>>>>>>>")
      if (PrePostAggegation.SaveRCCConnectionData(spark, jobId, processType, commonConfigParams) == false) {

        logger.info("NORRC CONNECTIION FOUND FOR GIVEN EARNFCN.....>>>>>>>>>>>>>>>>>>>>>>>>>>>>>end report>>>>>>>>>>>>>>")
      }

      //Report
      logger.info("REPORT TAB>>>>>>>>>>>>>>>>>>>>>>>>>>>>REPORT START>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      if (PrePostAggegation.saveReport(spark, jobId, processType, commonConfigParams) == false) {
        logger.info("NO REPORT Tab.....>>>>>>>>>>>>>>>>>>>>>>>>>>>>> report>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      } else
        logger.info("SUCCESSFULLY EXECUTE REPORT TAB END>>>>>>>>>>>>>>>>>>>>>>>>>>>>>end report>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        
      logger.info("RADIO PERFORMANCE DL AND UL >>>>>>>>> CONGESTION CHART START >>>>>>>>>>>>>>>>>>>>>>>>")  
      
      val conjectionMetadataMap:scala.collection.immutable.Map[String,List[CongestionData]]=CommonDaoImpl.getCongestionChartData(commonConfigParams)
      conjectionMetadataMap.foreach((congestionChartEntry) => {
        congestionChartEntry._2.foreach((congestioData) =>{
        PrePostAggegation.generateCongestionChart(spark,congestioData.groupKpiKey, congestioData.avgKpiKey, congestioData.normMaxValFlag, 
            congestioData.grpDefVal,congestioData.avgDefVal,jobId,processType,congestioData.reportName,
            commonConfigParams)
        })
      }
      )
      logger.info("RADIO PERFORMANCE DL AND UL >>>>>>>>> CONGESTION CHART END >>>>>>>>>>>>>>>>>>>>>>>>") 
      
      reportDataRdd.unpersist()
      
      
    } catch {
      case rde: ReportDataException =>
        logger.error("Main Catch : Report data exception : " + rde.getMessage + " :  ", rde)
        status = false
        CommonDaoImpl.updateJobStatus(jobId, "job_failed", rde.getMessage, commonConfigParams)
      case e: Exception =>
        logger.info("report data processing ended with exception for file " + processFileNames + " process is " + processType)
        logger.error("Main Catch : ES Exception occured while executing main block into ES : " + e.getMessage)
        println("Main Catch : ES Exception occured while executing main block into ES : " + e.getMessage)
        logger.error("Main Catch : ES Exception occured while executing main block into ES : " + e.printStackTrace())
        status = false
        CommonDaoImpl.updateJobStatus(jobId, "job_failed", e.getMessage, commonConfigParams)
    }finally{
      logger.info("unpersisting maprdd and reportDataRdd : jobid "+jobId)
      if(reportDataRdd!=null)
      reportDataRdd.unpersist()
      if(mapRdd!=null)
      mapRdd.unpersist()
      logger.info("unpersisted maprdd and reportDataRdd : jobid "+jobId)
    }
    logger.info("report data processing ended for file " + processFileNames + " process is " + processType)
    status
    /* Thread.sleep(sleepTime.toInt)
   }*/

  }

  def toDouble(in: Any): Option[Double] = {
    try {
      import java.lang.Double
      Some(Double.parseDouble(in.toString.trim()))
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toInteger(in: Any): Option[Integer] = {
    try {

      Some(Integer.parseInt(in.toString.trim()))
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toLong(in: Any): Option[Long] = {
    try {
     
      Some(java.lang.Long.parseLong(in.toString.trim()))
    } catch {
      case e: NumberFormatException => None
    }
  }

  import scala.collection.mutable._




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

  


    

  

  def getConnectionProperties(commonConfigParams: CommonConfigParameters) = {
    val connectionProperties: Properties = new Properties()
    connectionProperties.put("user", commonConfigParams.POSTGRES_DB_USER)
    connectionProperties.put("password", commonConfigParams.POSTGRES_DB_PWD)
    connectionProperties.put("driver", commonConfigParams.POSTGRES_DRIVER)
    connectionProperties
  }

}

case class ReportData(fileName: String, ltePccRsrp: Double, lTEPccSinr: Double, ltePccRsrq: Double, ltePccRssi: Double, lteBand: Integer, 
                      ltePccEarfcnDL: Integer, ltePccEarfcnUL: Integer,
                      ltePccPuschTxPwr: Double, ltePucchActTxPwr: Double, ltePuschTxPwr: Double, lteMacTputDl: Double, lteMacTputUl: Double,
                      ltePdschTput: Double, loc_2: String, lteCellID: Int, LTERlcTputDl: Double, LTERlcTputUl: Double, NCell10RSRP: Double,
                      NCell10RSRQ: Double, NCell10RSSI: Double, LTERrcState: String,
                      lteRrcConRej: Integer, lteRrcConRestRej: Integer, lteRrcConRelCnt: Integer, lteActDefContAcptCnt: Integer, 
                      lteActDedContAcptCnt: Integer, lteBearerModAcptCnt: Integer, lteHoFailure: Integer, lteIntraHoFail: Integer,
                      lteAttRej: Integer, lteActDefContRejCnt: Integer, lteActDedContRejCnt: Integer, lteBearerModRejCnt: Integer, 
                      lteReselFromGsmUmtsFail: Integer, lteRrcConSetupCnt: Integer, lteRrcConReqCnt: Integer, ltePdnConntReqCnt: Integer, lteAttReqCnt: Integer,
                      lteRrcConSetupCmpCnt: Integer, lteAttAcptCnt: Integer, lteRrcConeconfCmpCnt: Integer,
                      lteRrcConReconfigCnt: Integer, lteDetachReqCnt: Integer, lteRrcConRestReqCnt: Integer, lteTaAcptCnt: Integer,
                      lteTaReqCnt: Integer, lteTaRej: Integer, lteAttCmpCnt: Integer,ltePccTputDL: Double, ltePccCqiCw0: Integer,
                      ltePccDlMcs: Double,ltePdschPrb: Double,ltePuschThroughput: Double, lteModUl: String,ltePuschPrb: Double)

case class ReportRow(fileName: String)

case class LteRaReport(jobid: Long, process_type: String, var_kpi_range: String, fixed_kpi_val: String, kpi_name: String, pdf: Double, cdf: Double, count: Long, bin_average: Double,
                       kpi_min_val: Double, kpi_max_val: Double, kpi_avg_val: Double, groupbyName: String, report_name: String, bin_percent: Double, distribution: Double)

class ReportDataException(
  private val message: String    = "",
  private val cause:   Throwable = None.orNull)
  extends RuntimeException(message, cause)
/*class RestEsClientManager {
  val client: RestHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("10.20.40.28", 9201, "http")).build())
}*/

